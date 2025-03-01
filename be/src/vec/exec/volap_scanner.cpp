// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "vec/exec/volap_scanner.h"

#include "vec/columns/column_complex.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_string.h"
#include "vec/columns/column_vector.h"
#include "vec/common/assert_cast.h"
#include "vec/core/block.h"
#include "vec/exec/volap_scan_node.h"
#include "vec/exprs/vexpr_context.h"
#include "vec/olap/block_reader.h"

namespace doris::vectorized {

VOlapScanner::VOlapScanner(RuntimeState* runtime_state, VOlapScanNode* parent, bool aggregation,
                           bool need_agg_finalize, const TPaloScanRange& scan_range)
        : OlapScanner(runtime_state, parent, aggregation, need_agg_finalize, scan_range) {
    _reader.reset(new BlockReader);
}

Status VOlapScanner::get_block(RuntimeState* state, vectorized::Block* block, bool* eof) {
    // only empty block should be here
    DCHECK(block->rows() == 0);

    int64_t raw_rows_threshold = raw_rows_read() + config::doris_scanner_row_num;
    if (!block->mem_reuse()) {
        for (const auto slot_desc : _tuple_desc->slots()) {
            block->insert(ColumnWithTypeAndName(slot_desc->get_empty_mutable_column(),
                                                    slot_desc->get_data_type_ptr(),
                                                    slot_desc->col_name()));
        }
    }

    do {
        // Read one block from block reader
        auto res = _reader->next_block_with_aggregation(block, nullptr, nullptr, eof);
        if (res != OLAP_SUCCESS) {
            std::stringstream ss;
            ss << "Internal Error: read storage fail. res=" << res
               << ", tablet=" << _tablet->full_name()
               << ", backend=" << BackendOptions::get_localhost();
            return Status::InternalError(ss.str());
        }
        _num_rows_read += block->rows();
        _update_realtime_counter();

        // If we reach end of this scanner, break
        if (UNLIKELY(*eof)) {
            DCHECK(block->rows() == 0);
            break;
        }

        VLOG_ROW << "VOlapScanner input row: " << _read_row_cursor.to_string();

        RETURN_IF_ERROR(
                VExprContext::filter_block(_vconjunct_ctx, block, _tuple_desc->slots().size()));

    } while (block->rows() == 0 && !(*eof) && raw_rows_read() < raw_rows_threshold);

    return Status::OK();
}

void VOlapScanner::_convert_row_to_block(std::vector<vectorized::MutableColumnPtr>* columns) {
    size_t slots_size = _query_slots.size();
    for (int i = 0; i < slots_size; ++i) {
        SlotDescriptor* slot_desc = _query_slots[i];
        auto cid = _return_columns[i];

        auto* column_ptr = (*columns)[i].get();
        if (slot_desc->is_nullable()) {
            auto* nullable_column = reinterpret_cast<ColumnNullable*>((*columns)[i].get());
            if (_read_row_cursor.is_null(cid)) {
                nullable_column->insert_data(nullptr, 0);
                continue;
            } else {
                nullable_column->get_null_map_data().push_back(0);
                column_ptr = &nullable_column->get_nested_column();
            }
        }

        char* ptr = (char*)_read_row_cursor.cell_ptr(cid);
        switch (slot_desc->type().type) {
        case TYPE_BOOLEAN: {
            assert_cast<ColumnVector<UInt8>*>(column_ptr)->insert_data(ptr, 0);
            break;
        }
        case TYPE_TINYINT: {
            assert_cast<ColumnVector<Int8>*>(column_ptr)->insert_data(ptr, 0);
            break;
        }
        case TYPE_SMALLINT: {
            assert_cast<ColumnVector<Int16>*>(column_ptr)->insert_data(ptr, 0);
            break;
        }
        case TYPE_INT: {
            assert_cast<ColumnVector<Int32>*>(column_ptr)->insert_data(ptr, 0);
            break;
        }
        case TYPE_BIGINT: {
            assert_cast<ColumnVector<Int64>*>(column_ptr)->insert_data(ptr, 0);
            break;
        }
        case TYPE_LARGEINT: {
            assert_cast<ColumnVector<Int128>*>(column_ptr)->insert_data(ptr, 0);
            break;
        }
        case TYPE_FLOAT: {
            assert_cast<ColumnVector<Float32>*>(column_ptr)->insert_data(ptr, 0);
            break;
        }
        case TYPE_DOUBLE: {
            assert_cast<ColumnVector<Float64>*>(column_ptr)->insert_data(ptr, 0);
            break;
        }
        case TYPE_CHAR: {
            Slice* slice = reinterpret_cast<Slice*>(ptr);
            assert_cast<ColumnString*>(column_ptr)
                    ->insert_data(slice->data, strnlen(slice->data, slice->size));
            break;
        }
        case TYPE_VARCHAR:
        case TYPE_STRING: {
            Slice* slice = reinterpret_cast<Slice*>(ptr);
            assert_cast<ColumnString*>(column_ptr)->insert_data(slice->data, slice->size);
            break;
        }
        case TYPE_OBJECT: {
            Slice* slice = reinterpret_cast<Slice*>(ptr);
            // insert_default()
            auto* target_column = assert_cast<ColumnBitmap*>(column_ptr);

            target_column->insert_default();
            BitmapValue* pvalue = nullptr;
            int pos = target_column->size() - 1;
            pvalue = &target_column->get_element(pos);

            if (slice->size != 0) {
                BitmapValue value;
                value.deserialize(slice->data);
                *pvalue = std::move(value);
            } else {
                *pvalue = std::move(*reinterpret_cast<BitmapValue*>(slice->data));
            }
            break;
        }
        case TYPE_HLL: {
            Slice* slice = reinterpret_cast<Slice*>(ptr);
            if (slice->size != 0) {
                assert_cast<ColumnString*>(column_ptr)->insert_data(slice->data, slice->size);
                // TODO: in vector exec engine, it is diffcult to set hll size = 0
                // so we have to serialize here. which will cause two problem
                //      1. some unnecessary mem malloc and delay mem release
                //      2. some unnecessary CPU cost in serialize
            } else {
                auto* dst_hll = reinterpret_cast<HyperLogLog*>(slice->data);
                std::string result(dst_hll->max_serialized_size(), '0');
                int size = dst_hll->serialize((uint8_t*)result.c_str());
                result.resize(size);
                assert_cast<ColumnString*>(column_ptr)->insert_data(result.c_str(), size);
            }
            break;
        }
        case TYPE_DECIMALV2: {
            int64_t int_value = *(int64_t*)(ptr);
            int32_t frac_value = *(int32_t*)(ptr + sizeof(int64_t));
            DecimalV2Value data(int_value, frac_value);
            assert_cast<ColumnDecimal<Decimal128>*>(column_ptr)
                    ->insert_data(reinterpret_cast<char*>(&data), 0);
            break;
        }
        case TYPE_DATETIME: {
            uint64_t value = *reinterpret_cast<uint64_t*>(ptr);
            DateTimeValue data(value);
            assert_cast<ColumnVector<Int128>*>(column_ptr)
                    ->insert_data(reinterpret_cast<char*>(&data), 0);
            break;
        }
        case TYPE_DATE: {
            uint64_t value = 0;
            value = *(unsigned char*)(ptr + 2);
            value <<= 8;
            value |= *(unsigned char*)(ptr + 1);
            value <<= 8;
            value |= *(unsigned char*)(ptr);
            DateTimeValue date;
            date.from_olap_date(value);
            assert_cast<ColumnVector<Int128>*>(column_ptr)
                    ->insert_data(reinterpret_cast<char*>(&date), 0);
            break;
        }
        default: {
            break;
        }
        }
    }
}
} // namespace doris::vectorized

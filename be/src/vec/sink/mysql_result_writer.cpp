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

#include "vec/sink/mysql_result_writer.h"

#include "runtime/buffer_control_block.h"
#include "runtime/runtime_state.h"
#include "util/date_func.h"
#include "util/mysql_row_buffer.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_vector.h"
#include "vec/common/assert_cast.h"
#include "vec/exprs/vexpr.h"
#include "vec/exprs/vexpr_context.h"

namespace doris {
namespace vectorized {
VMysqlResultWriter::VMysqlResultWriter(BufferControlBlock* sinker,
                                       const std::vector<VExprContext*>& output_vexpr_ctxs,
                                       RuntimeProfile* parent_profile)
        : VResultWriter(),
          _sinker(sinker),
          _output_vexpr_ctxs(output_vexpr_ctxs),
          _parent_profile(parent_profile) {}

VMysqlResultWriter::~VMysqlResultWriter() {
    for (auto buffer : _vec_buffers) {
        delete buffer;
    }
}

Status VMysqlResultWriter::init(RuntimeState* state) {
    _init_profile();
    if (nullptr == _sinker) {
        return Status::InternalError("sinker is NULL pointer.");
    }

    _vec_buffers.resize(state->batch_size());
    for (int i = 0; i < state->batch_size(); ++i) {
        _vec_buffers[i] = new MysqlRowBuffer();
    }

    return Status::OK();
}

void VMysqlResultWriter::_init_profile() {
    _append_row_batch_timer = ADD_TIMER(_parent_profile, "AppendBatchTime");
    _convert_tuple_timer = ADD_CHILD_TIMER(_parent_profile, "TupleConvertTime", "AppendBatchTime");
    _result_send_timer = ADD_CHILD_TIMER(_parent_profile, "ResultRendTime", "AppendBatchTime");
    _sent_rows_counter = ADD_COUNTER(_parent_profile, "NumSentRows", TUnit::UNIT);
}

template <PrimitiveType type, bool is_nullable>
Status VMysqlResultWriter::_add_one_column(const ColumnPtr& column_ptr) {
    SCOPED_TIMER(_convert_tuple_timer);
    DCHECK(column_ptr->size() <= _vec_buffers.size());

    const auto column_size = column_ptr->size();
    for (int i = 0; i < column_size; i++) {
        _vec_buffers[i]->reset();
    }

    doris::vectorized::ColumnPtr column;
    if constexpr (is_nullable) {
        column = assert_cast<const ColumnNullable&>(*column_ptr).get_nested_column_ptr();
    } else {
        column = column_ptr;
    }

    int buf_ret = 0;
    for (int i = 0; i < column_size; ++i) {
        if constexpr (is_nullable) {
            if (column_ptr->is_null_at(i)) {
                buf_ret = _vec_buffers[i]->push_null();
                continue;
            }
        }

        if constexpr (type == TYPE_BOOLEAN) {
            //todo here need to using uint after MysqlRowBuffer support it
            buf_ret = _vec_buffers[i]->push_tinyint(
                assert_cast<const ColumnVector<UInt8>&>(*column).get_data()[i]);
        }
        if constexpr (type == TYPE_TINYINT) {
            buf_ret = _vec_buffers[i]->push_tinyint(
                assert_cast<const ColumnVector<Int8>&>(*column).get_data()[i]);
        }
        if constexpr (type == TYPE_SMALLINT) {
            buf_ret = _vec_buffers[i]->push_smallint(
                    assert_cast<const ColumnVector<Int16>&>(*column).get_data()[i]);
        }
        if constexpr (type == TYPE_INT) {
            buf_ret = _vec_buffers[i]->push_int(
                    assert_cast<const ColumnVector<Int32>&>(*column).get_data()[i]);
        }
        if constexpr (type == TYPE_BIGINT) {
            buf_ret = _vec_buffers[i]->push_bigint(
                    assert_cast<const ColumnVector<Int64>&>(*column).get_data()[i]);
        }
        if constexpr (type == TYPE_LARGEINT) {
            auto v = LargeIntValue::to_string(
                    assert_cast<const ColumnVector<Int128>&>(*column).get_data()[i]);
            buf_ret = _vec_buffers[i]->push_string(v.c_str(), v.size());
        }
        if constexpr (type == TYPE_FLOAT) {
            buf_ret = _vec_buffers[i]->push_float(
                    assert_cast<const ColumnVector<Float32>&>(*column).get_data()[i]);
        }
        if constexpr (type == TYPE_DOUBLE) {
            buf_ret = _vec_buffers[i]->push_double(
                    assert_cast<const ColumnVector<Float64>&>(*column).get_data()[i]);
        }
        if constexpr (type == TYPE_TIME) {
            buf_ret = _vec_buffers[i]->push_time(
                    assert_cast<const ColumnVector<Float64>&>(*column).get_data()[i]);
        }
        if constexpr (type == TYPE_DATETIME) {
            char buf[64];
            auto time_num = assert_cast<const ColumnVector<Int128>&>(*column).get_data()[i];
            DateTimeValue time_val;
            memcpy(&time_val, &time_num, sizeof(Int128));
            // TODO(zhaochun), this function has core risk
            char* pos = time_val.to_string(buf);
            buf_ret = _vec_buffers[i]->push_string(buf, pos - buf - 1);
        }

        if constexpr (type == TYPE_OBJECT) {
            buf_ret = _vec_buffers[i]->push_null();
        }
        if constexpr (type == TYPE_VARCHAR) {
            const auto string_val = column->get_data_at(i);

            if (string_val.data == NULL) {
                if (string_val.size == 0) {
                    // 0x01 is a magic num, not useful actually, just for present ""
                    char* tmp_val = reinterpret_cast<char*>(0x01);
                    buf_ret = _vec_buffers[i]->push_string(tmp_val, string_val.size);
                } else {
                    buf_ret = _vec_buffers[i]->push_null();
                }
            } else {
                buf_ret = _vec_buffers[i]->push_string(string_val.data, string_val.size);
            }
        }
        if constexpr (type == TYPE_DECIMALV2) {
            DecimalV2Value decimal_val(
                    assert_cast<const ColumnDecimal<Decimal128>&>(*column).get_data()[i]);
            auto decimal_str = decimal_val.to_string();
            buf_ret = _vec_buffers[i]->push_string(decimal_str.c_str(), decimal_str.length());
        }

        if (0 != buf_ret) {
            return Status::InternalError("pack mysql buffer failed.");
        }
    }

    return Status::OK();
}

Status VMysqlResultWriter::append_row_batch(const RowBatch* batch) {
    return Status::RuntimeError("Not Implemented MysqlResultWriter::append_row_batch scalar");
}

Status VMysqlResultWriter::append_block(Block& input_block) {
    SCOPED_TIMER(_append_row_batch_timer);
    Status status = Status::OK();
    if (UNLIKELY(input_block.rows() == 0)) { return status; }

    // Exec vectorized expr here to speed up, block.rows() == 0 means expr exec
    // failed, just return the error status
    auto block = VExprContext::get_output_block_after_execute_exprs(_output_vexpr_ctxs,
            input_block, status);
    auto num_rows = block.rows();
    if (UNLIKELY(num_rows == 0)) { return status; }

    // TODO: Recheck the vec buffer size of block buffer size
    if (UNLIKELY(num_rows > _vec_buffers.size())) {
        auto gap_size = num_rows - _vec_buffers.size();
        _vec_buffers.reserve(num_rows);
        for (int i = 0; i < gap_size; ++i) {
            _vec_buffers.emplace_back(new MysqlRowBuffer);
        }
    }

    // convert one batch
    auto result = std::make_unique<TFetchDataResult>();
    result->result_batch.rows.resize(num_rows);
    for (int i = 0; status.ok() && i < _output_vexpr_ctxs.size(); ++i) {
        auto column_ptr =
                block.get_by_position(i).column->convert_to_full_column_if_const();
        auto type_ptr = block.get_by_position(i).type;

        switch (_output_vexpr_ctxs[i]->root()->result_type()) {
        case TYPE_BOOLEAN:
            if (type_ptr->is_nullable()) {
                status = _add_one_column<PrimitiveType::TYPE_BOOLEAN, true>(column_ptr);
            } else {
                status = _add_one_column<PrimitiveType::TYPE_BOOLEAN, false>(column_ptr);
            }
            break;
        case TYPE_TINYINT: {
            if (type_ptr->is_nullable()) {
                status = _add_one_column<PrimitiveType::TYPE_TINYINT, true>(column_ptr);
            } else {
                status = _add_one_column<PrimitiveType::TYPE_TINYINT, false>(column_ptr);
            }
            break;
        }
        case TYPE_SMALLINT: {
            if (type_ptr->is_nullable()) {
                status = _add_one_column<PrimitiveType::TYPE_SMALLINT, true>(column_ptr);
            } else {
                status = _add_one_column<PrimitiveType::TYPE_SMALLINT, false>(column_ptr);
            }
            break;
        }
        case TYPE_INT: {
            if (type_ptr->is_nullable()) {
                status = _add_one_column<PrimitiveType::TYPE_INT, true>(column_ptr);
            } else {
                status = _add_one_column<PrimitiveType::TYPE_INT, false>(column_ptr);
            }
            break;
        }
        case TYPE_BIGINT: {
            if (type_ptr->is_nullable()) {
                status = _add_one_column<PrimitiveType::TYPE_BIGINT, true>(column_ptr);
            } else {
                status = _add_one_column<PrimitiveType::TYPE_BIGINT, false>(column_ptr);
            }
            break;
        }
        case TYPE_LARGEINT: {
            if (type_ptr->is_nullable()) {
                status = _add_one_column<PrimitiveType::TYPE_LARGEINT, true>(column_ptr);
            } else {
                status = _add_one_column<PrimitiveType::TYPE_LARGEINT, false>(column_ptr);
            }
            break;
        }
        case TYPE_FLOAT: {
            if (type_ptr->is_nullable()) {
                status = _add_one_column<PrimitiveType::TYPE_FLOAT, true>(column_ptr);
            } else {
                status = _add_one_column<PrimitiveType::TYPE_FLOAT, false>(column_ptr);
            }
            break;
        }
        case TYPE_DOUBLE: {
            if (type_ptr->is_nullable()) {
                status = _add_one_column<PrimitiveType::TYPE_DOUBLE, true>(column_ptr);
            } else {
                status = _add_one_column<PrimitiveType::TYPE_DOUBLE, false>(column_ptr);
            }
            break;
        }
        case TYPE_TIME: {
            if (type_ptr->is_nullable()) {
                status = _add_one_column<PrimitiveType::TYPE_TIME, true>(column_ptr);
            } else {
                status = _add_one_column<PrimitiveType::TYPE_TIME, false>(column_ptr);
            }
            break;
        }
        case TYPE_STRING:
        case TYPE_CHAR:
        case TYPE_VARCHAR: {
            if (type_ptr->is_nullable()) {
                status = _add_one_column<PrimitiveType::TYPE_VARCHAR, true>(column_ptr);
            } else {
                status = _add_one_column<PrimitiveType::TYPE_VARCHAR, false>(column_ptr);
            }
            break;
        }
        case TYPE_DECIMALV2: {
            if (type_ptr->is_nullable()) {
                status = _add_one_column<PrimitiveType::TYPE_DECIMALV2, true>(column_ptr);
            } else {
                status = _add_one_column<PrimitiveType::TYPE_DECIMALV2, false>(column_ptr);
            }
            break;
        }
        case TYPE_DATE:
        case TYPE_DATETIME: {
            if (type_ptr->is_nullable()) {
                status = _add_one_column<PrimitiveType::TYPE_DATETIME, true>(column_ptr);
            } else {
                status = _add_one_column<PrimitiveType::TYPE_DATETIME, false>(column_ptr);
            }
            break;
        }
        case TYPE_HLL:
        case TYPE_OBJECT: {
            if (type_ptr->is_nullable()) {
                status = _add_one_column<PrimitiveType::TYPE_OBJECT, true>(column_ptr);
            } else {
                status = _add_one_column<PrimitiveType::TYPE_OBJECT, false>(column_ptr);
            }
            break;
        }
        default: {
            LOG(WARNING) << "can't convert this type to mysql type. type = "
                         << _output_vexpr_ctxs[i]->root()->type();
            return Status::InternalError("vec block pack mysql buffer failed.");
        }
        }

        if (status) {
            for (int j = 0; j < num_rows; ++j) {
                result->result_batch.rows[j].append(_vec_buffers[j]->buf(),
                                                    _vec_buffers[j]->length());
            }
        } else {
            LOG(WARNING) << "convert row to mysql result failed.";
            break;
        }
    }
    if (status) {
        SCOPED_TIMER(_result_send_timer);
        // push this batch to back
        status = _sinker->add_batch(result);

        if (status.ok()) {
            _written_rows += num_rows;
        } else {
            LOG(WARNING) << "append result batch to sink failed.";
        }
    }

    return status;
}

Status VMysqlResultWriter::close() {
    COUNTER_SET(_sent_rows_counter, _written_rows);
    return Status::OK();
}

} // namespace vectorized
} // namespace doris

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

#include "vec/exec/vsort_node.h"

#include "exec/sort_exec_exprs.h"
#include "runtime/row_batch.h"
#include "runtime/runtime_state.h"
//#include "runtime/sorted_run_merger.h"
#include "util/debug_util.h"

#include "vec/core/sort_block.h"

namespace doris::vectorized {

VSortNode::VSortNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
        : ExecNode(pool, tnode, descs),
          _offset(tnode.sort_node.__isset.offset ? tnode.sort_node.offset : 0),
          _num_rows_skipped(0) {}

VSortNode::~VSortNode() {}

Status VSortNode::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::init(tnode, state));
    RETURN_IF_ERROR(_vsort_exec_exprs.init(tnode.sort_node.sort_info, _pool));
    _is_asc_order = tnode.sort_node.sort_info.is_asc_order;
    _nulls_first = tnode.sort_node.sort_info.nulls_first;
    return Status::OK();
}

Status VSortNode::prepare(RuntimeState* state) {
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    RETURN_IF_ERROR(ExecNode::prepare(state));
    RETURN_IF_ERROR(_vsort_exec_exprs.prepare(state, child(0)->row_desc(), _row_descriptor,
                                              expr_mem_tracker()));
    // AddExprCtxsToFree(_sort_exec_exprs);
    return Status::OK();
}

Status VSortNode::open(RuntimeState* state) {
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    RETURN_IF_ERROR(ExecNode::open(state));
    RETURN_IF_ERROR(_vsort_exec_exprs.open(state));
    RETURN_IF_CANCELLED(state);
    RETURN_IF_ERROR(state->check_query_state("vsort, while open."));
    RETURN_IF_ERROR(child(0)->open(state));

    // The child has been opened and the sorter created. Sort the input.
    // The final merge is done on-demand as rows are requested in get_next().
    RETURN_IF_ERROR(sort_input(state));

    // Unless we are inside a subplan expecting to call open()/get_next() on the child
    // again, the child can be closed at this point.
    // if (!IsInSubplan()) {
    child(0)->close(state);
    // }
    return Status::OK();
}

Status VSortNode::get_next(RuntimeState* state, RowBatch* row_batch, bool* eos) {
    row_batch = nullptr;
    *eos = true;
    return Status::NotSupported("Not Implemented VSortNode::get_next scalar");
}

Status VSortNode::get_next(RuntimeState* state, Block* block, bool* eos) {
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    if (_sorted_blocks.empty()) {
        *eos = true;
        return Status::OK();
    } else if (_sorted_blocks.size() == 1) {
        block->swap(_sorted_blocks[0]);
        _sorted_blocks.clear();
        return Status::OK();
    }

    auto status = merge_sort_read(state, block, eos);
    if (*eos) COUNTER_SET(_rows_returned_counter, _num_rows_returned);

    return status;
}

Status VSortNode::reset(RuntimeState* state) {
    _num_rows_skipped = 0;
    return Status::OK();
}

Status VSortNode::close(RuntimeState* state) {
    if (is_closed()) {
        return Status::OK();
    }
    _vsort_exec_exprs.close(state);
    ExecNode::close(state);
    return Status::OK();
}

void VSortNode::debug_string(int indentation_level, stringstream* out) const {
    *out << string(indentation_level * 2, ' ');
//    *out << "SortNode(" << Expr::debug_string(_vsort_exec_exprs.lhs_ordering_expr_ctxs());
    for (int i = 0; i < _is_asc_order.size(); ++i) {
        *out << (i > 0 ? " " : "") << (_is_asc_order[i] ? "asc" : "desc") << " nulls "
             << (_nulls_first[i] ? "first" : "last");
    }
    ExecNode::debug_string(indentation_level, out);
    *out << ")";
}

Status VSortNode::sort_input(RuntimeState* state) {
    bool eos = false;
    do {
        Block block;
        RETURN_IF_ERROR(child(0)->get_next(state, &block, &eos));
        if ( block.rows() != 0) {
            RETURN_IF_ERROR(pretreat_block(block));
            _sorted_blocks.emplace_back(std::move(block));
            RETURN_IF_CANCELLED(state);
            RETURN_IF_ERROR(state->check_query_state("vsort, while sorting input."));
        }
    } while (!eos);

    build_merge_tree();
    return Status::OK();
}

Status VSortNode::pretreat_block(doris::vectorized::Block& block) {
    if (_vsort_exec_exprs.need_materialize_tuple()) {
        auto output_tuple_expr_ctxs = _vsort_exec_exprs.sort_tuple_slot_expr_ctxs();
        std::vector<int> valid_column_ids(output_tuple_expr_ctxs.size());
        for (int i = 0; i < output_tuple_expr_ctxs.size(); ++i) {
            RETURN_IF_ERROR(output_tuple_expr_ctxs[i]->execute(&block, &valid_column_ids[i]));
        }

        Block new_block;
        for (auto column_id : valid_column_ids) {
            new_block.insert(block.getByPosition(column_id));
        }
        block.swap(new_block);
    }

    _sort_description.resize(_vsort_exec_exprs.lhs_ordering_expr_ctxs().size());
    for (int i = 0; i < _sort_description.size(); i++) {
        const auto &ordering_expr = _vsort_exec_exprs.lhs_ordering_expr_ctxs()[i];
        RETURN_IF_ERROR(ordering_expr->execute(&block, &_sort_description[i].column_number));

        _sort_description[i].direction = _is_asc_order[i] ? 1 : -1;
        _sort_description[i].nulls_direction = _nulls_first[i] ? 1 : -1;
    }

    sortBlock(block, _sort_description, _offset + _limit);

    return Status::OK();
}

void VSortNode::build_merge_tree() {
    std::vector<Block> nonempty_blocks;
    for (const auto &block : _sorted_blocks) {
        if (block.rows() == 0)
            continue;

        nonempty_blocks.push_back(block);
        _cursors.emplace_back(block, _sort_description);
    }

    _sorted_blocks.swap(nonempty_blocks);

    if (_sorted_blocks.size() > 1) {
        for (size_t i = 0; i < _cursors.size(); ++i)
            _priority_queue.push(SortCursor(&_cursors[i]));
    }
}

Status VSortNode::merge_sort_read(doris::RuntimeState *state, doris::vectorized::Block *block, bool *eos) {
    size_t num_columns = _sorted_blocks[0].columns();

    MutableColumns merged_columns = _sorted_blocks[0].cloneEmptyColumns();
    /// TODO: reserve (in each column)

    /// Take rows from queue in right order and push to 'merged'.
    size_t merged_rows = 0;
    while (!_priority_queue.empty()) {
        auto current = _priority_queue.top();
        _priority_queue.pop();

        for (size_t i = 0; i < num_columns; ++i)
            merged_columns[i]->insertFrom(*current->all_columns[i], current->pos);

        if (!current->isLast()) {
            current->next();
            _priority_queue.push(current);
        }

        ++merged_rows;
        if (merged_rows == state->batch_size())
            break;
    }

    if (merged_rows == 0) {
        *eos = true;
        return Status::OK();
    }

    Block merge_block = _sorted_blocks[0].cloneWithColumns(std::move(merged_columns));
    _num_rows_returned += merge_block.rows();
    if (reached_limit()) {
        merge_block.set_num_rows(merge_block.rows() - (_num_rows_returned - _limit));
        *eos = true;
    }
    merge_block.swap(*block);

    return Status::OK();
}

} // end namespace doris
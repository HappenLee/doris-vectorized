#pragma once
#include "vec/sink/data_sink.h"
#include "vec/sink/result_writer.h"

namespace doris {
class ObjectPool;
class RowBatch;
class RuntimeState;
class RuntimeProfile;
class BufferControlBlock;
class ExprContext;
class ResultWriter;
class MemTracker;
class ResultFileOptions;
namespace vectorized {
class VExprContext;
class ResultSink : public VDataSink {
public:
    // construct a buffer for the result need send to coordinator.
    // row_desc used for convert RowBatch to TRowBatch
    // buffer_size is the buffer size allocated to each query
    ResultSink(const RowDescriptor& row_desc, const std::vector<TExpr>& select_exprs,
               const TResultSink& sink, int buffer_size);

    virtual ~ResultSink();

    virtual Status prepare(RuntimeState* state) override;
    virtual Status open(RuntimeState* state) override;

    // not implement
    virtual Status send(RuntimeState* state, RowBatch* batch) override;
    virtual Status send(RuntimeState* state, Block* block) override;
    // Flush all buffered data and close all existing channels to destination
    // hosts. Further send() calls are illegal after calling close().
    virtual Status close(RuntimeState* state, Status exec_status) override;
    virtual RuntimeProfile* profile() override { return _profile; }

    void set_query_statistics(std::shared_ptr<QueryStatistics> statistics) override;

private:
    Status prepare_exprs(RuntimeState* state);
    TResultSinkType::type _sink_type;
    // set file options when sink type is FILE
    std::unique_ptr<ResultFileOptions> _file_opts;

    ObjectPool* _obj_pool;
    // Owned by the RuntimeState.
    const RowDescriptor& _row_desc;

    // Owned by the RuntimeState.
    const std::vector<TExpr>& _t_output_expr;
    std::vector<vectorized::VExprContext*> _output_vexpr_ctxs;

    boost::shared_ptr<BufferControlBlock> _sender;
    boost::shared_ptr<VResultWriter> _writer;
    RuntimeProfile* _profile; // Allocated from _pool
    int _buf_size;            // Allocated from _pool
};
} // namespace vectorized

} // namespace doris
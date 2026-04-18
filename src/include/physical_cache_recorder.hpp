#pragma once

#include "concurrency/annotated_mutex.hpp"
#include "concurrency/thread_annotation.hpp"
#include "query_condition_cache_state.hpp"

#include "duckdb/common/optional_idx.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/planner/expression.hpp"

namespace duckdb {

struct CacheRecorderLocalState : public OperatorState {
	CacheRecorderLocalState(ClientContext &context, const Expression &bound_predicate);

	// Co-owned with the global state so bitvectors outlive this OperatorState.
	shared_ptr<ConditionCacheEntry> local_entry;
	ExpressionExecutor expr_executor;
	// rg_idx -> highest vec_idx this task has seen.
	unordered_map<idx_t, idx_t> max_vec_per_rg;
	optional_idx current_rg;
};

struct CacheRecorderGlobalState : public GlobalOperatorState {
	concurrency::mutex lock;
	vector<shared_ptr<ConditionCacheEntry>> task_local_entries DUCKDB_GUARDED_BY(lock);
};

// Pass-through operator injected above LogicalGet on cache miss. Observes scan output,
// evaluates the predicate per chunk, and merges thread-local bitvectors into the store
// at OperatorFinalize. The last vec each task observes is left uncommitted because it
// may be a partial tail.
class PhysicalCacheRecorder : public PhysicalOperator {
public:
	PhysicalCacheRecorder(PhysicalPlan &physical_plan, idx_t table_oid_p, string canonical_key_p,
	                      unique_ptr<Expression> bound_predicate_p, idx_t rowid_column_index_p,
	                      vector<LogicalType> types, idx_t estimated_cardinality);

	idx_t table_oid;
	string canonical_key;
	unique_ptr<Expression> bound_predicate;
	idx_t rowid_column_index;

	unique_ptr<GlobalOperatorState> GetGlobalOperatorState(ClientContext &context) const override;
	unique_ptr<OperatorState> GetOperatorState(ExecutionContext &context) const override;
	OperatorResultType Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
	                           GlobalOperatorState &gstate, OperatorState &state) const override;
	OperatorFinalResultType OperatorFinalize(Pipeline &pipeline, Event &event, ClientContext &context,
	                                         OperatorFinalizeInput &input) const override;
	bool RequiresOperatorFinalize() const override;
	bool ParallelOperator() const override;
	string GetName() const override;
	InsertionOrderPreservingMap<string> ParamsToString() const override;

	// Public so unit tests can drive the watermark/bitvector algorithm with synthetic
	// (rg, vec, qualifying) tuples without a real pipeline.
	static void RecordChunkObservation(ConditionCacheEntry &local_entry, unordered_map<idx_t, idx_t> &max_vec_per_rg,
	                                   optional_idx &current_rg, idx_t rg_idx, idx_t vec_idx, bool has_qualifying);
};

} // namespace duckdb

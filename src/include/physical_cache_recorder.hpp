#pragma once

#include "concurrency/annotated_mutex.hpp"
#include "concurrency/thread_annotation.hpp"
#include "query_condition_cache_state.hpp"

#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/planner/expression.hpp"

namespace duckdb {

struct CacheRecorderLocalState : public OperatorState {
	CacheRecorderLocalState(ClientContext &context, const Expression &bound_predicate);

	shared_ptr<ConditionCacheEntry> local_entry;
	ExpressionExecutor expr_executor;
};

struct CacheRecorderGlobalState : public GlobalOperatorState {
	concurrency::mutex lock;
	vector<shared_ptr<ConditionCacheEntry>> task_local_entries DUCKDB_GUARDED_BY(lock);
};

class PhysicalCacheRecorder : public PhysicalOperator {
public:
	PhysicalCacheRecorder(PhysicalPlan &physical_plan, idx_t table_oid_p, string canonical_key_p,
	                      unique_ptr<Expression> bound_predicate_p, idx_t rowid_column_index_p, string table_catalog_p,
	                      string table_schema_p, string table_name_p, unique_ptr<Expression> backfill_predicate_p,
	                      shared_ptr<ConditionCacheEntry> cache_entry_p,
	                      shared_ptr<ConditionCacheEntry> metadata_entry_p, vector<LogicalType> types,
	                      idx_t estimated_cardinality);

	idx_t table_oid;
	string canonical_key;
	string table_catalog;
	string table_schema;
	string table_name;
	unique_ptr<Expression> bound_predicate;
	unique_ptr<Expression> backfill_predicate;
	idx_t rowid_column_index;
	shared_ptr<ConditionCacheEntry> cache_entry;
	shared_ptr<ConditionCacheEntry> metadata_entry;

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

	static void RecordChunkObservation(ConditionCacheEntry &local_entry, idx_t rg_idx, idx_t vec_idx,
	                                   bool has_qualifying);
};

} // namespace duckdb

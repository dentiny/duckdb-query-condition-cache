#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include "query_condition_cache_state.hpp"

namespace duckdb {

// Filter operator used by the fused-build path. The child input contains the
// normal visible columns plus a hidden ROW_ID column; this operator evaluates
// the predicate over the input and emits only the visible columns.
class PhysicalCacheBuildingFilter : public PhysicalOperator {
public:
	PhysicalCacheBuildingFilter(PhysicalPlan &physical_plan, vector<LogicalType> visible_types,
	                            vector<unique_ptr<Expression>> select_list, idx_t row_id_column_index,
	                            bool hide_row_id_column, shared_ptr<ConditionCacheEntry> cache_entry,
	                            string table_catalog, string table_schema, string table_name,
	                            unique_ptr<Expression> backfill_predicate, bool allow_finalize_backfill,
	                            idx_t estimated_cardinality);

	vector<unique_ptr<Expression>> expression_list;
	idx_t row_id_column_index;
	bool hide_row_id_column;
	shared_ptr<ConditionCacheEntry> cache_entry;
	string table_catalog;
	string table_schema;
	string table_name;
	unique_ptr<Expression> backfill_predicate;
	bool allow_finalize_backfill;

	unique_ptr<OperatorState> GetOperatorState(ExecutionContext &context) const override;
	OperatorResultType Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
	                           GlobalOperatorState &gstate, OperatorState &state) const override;
	OperatorFinalResultType OperatorFinalize(Pipeline &pipeline, Event &event, ClientContext &context,
	                                         OperatorFinalizeInput &input) const override;
	bool RequiresOperatorFinalize() const override;
	string GetName() const override;
	InsertionOrderPreservingMap<string> ParamsToString() const override;
};

} // namespace duckdb

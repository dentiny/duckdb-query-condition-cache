#pragma once

#include "duckdb/planner/operator/logical_extension_operator.hpp"
#include "physical_cache_invalidator.hpp"

namespace duckdb {

struct LogicalCacheInvalidator : public LogicalExtensionOperator {
	idx_t table_oid;
	CacheInvalidatorMode mode;
	idx_t row_id_column_index;  // for ROW_ID mode
	bool row_id_is_last_column; // for UPDATE (resolve in CreatePlan)
	idx_t pre_insert_row_count; // for INSERT/MERGE modes

	// For DELETE: pass the row_id expression to be resolved during column binding.
	LogicalCacheInvalidator(idx_t table_oid, unique_ptr<Expression> row_id_expr);

	// For UPDATE: row_id is always the last column of the child output.
	explicit LogicalCacheInvalidator(idx_t table_oid);

	// For INSERT: count rows and compute affected range.
	LogicalCacheInvalidator(idx_t table_oid, idx_t pre_insert_row_count);

	// For MERGE: hybrid — track row IDs (at row_id_column_index) for matched rows
	// and count unmatched rows for the insert range.
	LogicalCacheInvalidator(idx_t table_oid, idx_t row_id_column_index, idx_t pre_insert_row_count);

	PhysicalOperator &CreatePlan(ClientContext &context, PhysicalPlanGenerator &planner) override;
	vector<ColumnBinding> GetColumnBindings() override;

protected:
	void ResolveTypes() override;
};

} // namespace duckdb

#include "logical_cache_invalidator.hpp"

#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"

namespace duckdb {

// DELETE mode: row_id expression stored in expressions[0], resolved during column binding
LogicalCacheInvalidator::LogicalCacheInvalidator(idx_t table_oid, unique_ptr<Expression> row_id_expr)
    : table_oid(table_oid), mode(CacheInvalidatorMode::ROW_ID), row_id_column_index(0), row_id_is_last_column(false),
      pre_insert_row_count(0) {
	expressions.push_back(std::move(row_id_expr));
}

// UPDATE mode: row_id is always the last column
LogicalCacheInvalidator::LogicalCacheInvalidator(idx_t table_oid)
    : table_oid(table_oid), mode(CacheInvalidatorMode::ROW_ID), row_id_column_index(0), row_id_is_last_column(true),
      pre_insert_row_count(0) {
}

// INSERT mode: count rows, no row_id tracking
LogicalCacheInvalidator::LogicalCacheInvalidator(idx_t table_oid, idx_t pre_insert_row_count)
    : table_oid(table_oid), mode(CacheInvalidatorMode::INSERT), row_id_column_index(0), row_id_is_last_column(false),
      pre_insert_row_count(pre_insert_row_count) {
}

// MERGE mode: track row IDs + count unmatched (inserted) rows
LogicalCacheInvalidator::LogicalCacheInvalidator(idx_t table_oid, idx_t row_id_column_index, idx_t pre_insert_row_count)
    : table_oid(table_oid), mode(CacheInvalidatorMode::MERGE), row_id_column_index(row_id_column_index),
      row_id_is_last_column(false), pre_insert_row_count(pre_insert_row_count) {
}

PhysicalOperator &LogicalCacheInvalidator::CreatePlan(ClientContext &context, PhysicalPlanGenerator &planner) {
	auto &child_plan = planner.CreatePlan(*children[0]);

	idx_t resolved_row_id_col = row_id_column_index;
	if (mode == CacheInvalidatorMode::ROW_ID) {
		if (row_id_is_last_column) {
			resolved_row_id_col = child_plan.types.size() - 1;
		} else {
			auto &bound_ref = expressions[0]->Cast<BoundReferenceExpression>();
			resolved_row_id_col = bound_ref.index;
		}
	}

	auto &op = planner.Make<PhysicalCacheInvalidator>(table_oid, mode, resolved_row_id_col, pre_insert_row_count,
	                                                  child_plan.types, estimated_cardinality);
	op.children.push_back(child_plan);
	return op;
}

vector<ColumnBinding> LogicalCacheInvalidator::GetColumnBindings() {
	return children[0]->GetColumnBindings();
}

void LogicalCacheInvalidator::ResolveTypes() {
	types = children[0]->types;
}

} // namespace duckdb

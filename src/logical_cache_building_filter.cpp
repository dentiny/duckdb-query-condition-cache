#include "logical_cache_building_filter.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"

namespace duckdb {

LogicalCacheBuildingFilter::LogicalCacheBuildingFilter(vector<unique_ptr<Expression>> filter_expressions_p,
                                                       unique_ptr<Expression> row_id_reference_p,
                                                       idx_t row_id_column_index_p, bool hide_row_id_column_p,
                                                       shared_ptr<ConditionCacheEntry> cache_entry_p)
    : row_id_column_index(row_id_column_index_p), hide_row_id_column(hide_row_id_column_p),
      filter_expression_count(filter_expressions_p.size()), cache_entry(std::move(cache_entry_p)) {
	expressions = std::move(filter_expressions_p);
	expressions.push_back(std::move(row_id_reference_p));
}

PhysicalOperator &LogicalCacheBuildingFilter::CreatePlan(ClientContext &context, PhysicalPlanGenerator &planner) {
	auto &child_plan = planner.CreatePlan(*children[0]);

	idx_t resolved_row_id_column_index;
	if (TryResolveRowIdColumnIndex(resolved_row_id_column_index)) {
		row_id_column_index = resolved_row_id_column_index;
	}

	vector<unique_ptr<Expression>> filter_expressions;
	filter_expressions.reserve(filter_expression_count);
	for (idx_t i = 0; i < filter_expression_count; ++i) {
		filter_expressions.push_back(std::move(expressions[i]));
	}

	auto &op = planner.Make<PhysicalCacheBuildingFilter>(types, std::move(filter_expressions), row_id_column_index,
	                                                     hide_row_id_column, cache_entry, estimated_cardinality);
	op.children.push_back(child_plan);
	return op;
}

vector<ColumnBinding> LogicalCacheBuildingFilter::GetColumnBindings() {
	auto child_bindings = children[0]->GetColumnBindings();
	if (hide_row_id_column) {
		idx_t resolved_row_id_column_index;
		if (TryResolveRowIdColumnIndex(resolved_row_id_column_index)) {
			row_id_column_index = resolved_row_id_column_index;
		}
		D_ASSERT(row_id_column_index < child_bindings.size());
		if (row_id_column_index < child_bindings.size()) {
			child_bindings.erase(child_bindings.begin() + NumericCast<int64_t>(row_id_column_index));
		}
	}
	return child_bindings;
}

void LogicalCacheBuildingFilter::ResolveTypes() {
	children[0]->ResolveOperatorTypes();
	types = children[0]->types;
	idx_t resolved_row_id_column_index;
	if (TryResolveRowIdColumnIndex(resolved_row_id_column_index)) {
		row_id_column_index = resolved_row_id_column_index;
	}
	if (row_id_column_index >= types.size()) {
		throw InternalException(
		    "CACHE_BUILDING_FILTER row_id_column_index %llu out of bounds for child type count %llu",
		    row_id_column_index, types.size());
	}
	if (hide_row_id_column) {
		types.erase(types.begin() + NumericCast<int64_t>(row_id_column_index));
	}
}

bool LogicalCacheBuildingFilter::TryResolveRowIdColumnIndex(idx_t &resolved_index) const {
	if (filter_expression_count >= expressions.size()) {
		return false;
	}

	auto &row_id_expr = expressions[filter_expression_count];
	if (row_id_expr->GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF) {
		return false;
	}

	auto row_id_binding = row_id_expr->Cast<BoundColumnRefExpression>().binding;
	auto child_bindings = children[0]->GetColumnBindings();
	for (idx_t child_idx = 0; child_idx < child_bindings.size(); ++child_idx) {
		if (child_bindings[child_idx] == row_id_binding) {
			resolved_index = child_idx;
			return true;
		}
	}
	return false;
}

string LogicalCacheBuildingFilter::GetExtensionName() const {
	return "query_condition_cache";
}

void LogicalCacheBuildingFilter::Serialize(Serializer &serializer) const {
	LogicalExtensionOperator::Serialize(serializer);
	serializer.WriteProperty(400, "row_id_column_index", row_id_column_index);
	serializer.WritePropertyWithDefault(401, "expressions", expressions);
	serializer.WriteProperty(402, "hide_row_id_column", hide_row_id_column);
}

} // namespace duckdb

#include "logical_cache_invalidator.hpp"

#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"

namespace duckdb {

// DELETE/UPDATE mode: row_id expression stored in expressions[0], resolved during column binding
LogicalCacheInvalidator::LogicalCacheInvalidator(idx_t table_oid, unique_ptr<Expression> row_id_expr)
    : table_oid(table_oid), mode(CacheInvalidatorMode::ROW_ID), row_id_column_index(0), pre_insert_row_count(0) {
	expressions.push_back(std::move(row_id_expr));
}

// INSERT mode: count rows, no row_id tracking
LogicalCacheInvalidator::LogicalCacheInvalidator(idx_t table_oid, idx_t pre_insert_row_count)
    : table_oid(table_oid), mode(CacheInvalidatorMode::INSERT), row_id_column_index(0),
      pre_insert_row_count(pre_insert_row_count) {
}

// MERGE mode: track row IDs + count unmatched (inserted) rows
LogicalCacheInvalidator::LogicalCacheInvalidator(idx_t table_oid, idx_t row_id_column_index, idx_t pre_insert_row_count)
    : table_oid(table_oid), mode(CacheInvalidatorMode::MERGE), row_id_column_index(row_id_column_index),
      pre_insert_row_count(pre_insert_row_count) {
}

PhysicalOperator &LogicalCacheInvalidator::CreatePlan(ClientContext &context, PhysicalPlanGenerator &planner) {
	auto &child_plan = planner.CreatePlan(*children[0]);

	idx_t resolved_row_id_col = row_id_column_index;
	if (mode == CacheInvalidatorMode::ROW_ID) {
		auto &bound_ref = expressions[0]->Cast<BoundReferenceExpression>();
		resolved_row_id_col = bound_ref.index;
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

string LogicalCacheInvalidator::GetExtensionName() const {
	return "query_condition_cache";
}

void LogicalCacheInvalidator::Serialize(Serializer &serializer) const {
	LogicalExtensionOperator::Serialize(serializer);
	serializer.WriteProperty(300, "table_oid", table_oid);
	serializer.WriteProperty(301, "mode", static_cast<uint8_t>(mode));
	serializer.WriteProperty(302, "row_id_column_index", row_id_column_index);
	serializer.WriteProperty(303, "pre_insert_row_count", pre_insert_row_count);
	serializer.WritePropertyWithDefault(304, "expressions", expressions);
}

// --- CacheInvalidatorOperatorExtension ---

static BoundStatement CacheInvalidatorBind(ClientContext &context, Binder &binder, OperatorExtensionInfo *info,
                                           SQLStatement &statement) {
	// We don't bind any statements — return empty plan to signal "not handled"
	return BoundStatement();
}

CacheInvalidatorOperatorExtension::CacheInvalidatorOperatorExtension() {
	Bind = CacheInvalidatorBind;
}

std::string CacheInvalidatorOperatorExtension::GetName() {
	return "query_condition_cache";
}

unique_ptr<LogicalExtensionOperator> CacheInvalidatorOperatorExtension::Deserialize(Deserializer &deserializer) {
	auto oid = deserializer.ReadProperty<idx_t>(300, "table_oid");
	auto mode_val = deserializer.ReadProperty<uint8_t>(301, "mode");
	auto row_id_col = deserializer.ReadProperty<idx_t>(302, "row_id_column_index");
	auto pre_insert = deserializer.ReadProperty<idx_t>(303, "pre_insert_row_count");
	auto exprs = deserializer.ReadPropertyWithDefault<vector<unique_ptr<Expression>>>(304, "expressions");

	auto mode = static_cast<CacheInvalidatorMode>(mode_val);

	unique_ptr<LogicalCacheInvalidator> result;
	switch (mode) {
	case CacheInvalidatorMode::ROW_ID: {
		unique_ptr<Expression> row_id_expr;
		if (!exprs.empty()) {
			row_id_expr = std::move(exprs[0]);
		} else {
			row_id_expr = make_uniq<BoundReferenceExpression>(LogicalType::BIGINT, row_id_col);
		}
		result = make_uniq<LogicalCacheInvalidator>(oid, std::move(row_id_expr));
		break;
	}
	case CacheInvalidatorMode::INSERT:
		result = make_uniq<LogicalCacheInvalidator>(oid, pre_insert);
		break;
	case CacheInvalidatorMode::MERGE:
		result = make_uniq<LogicalCacheInvalidator>(oid, row_id_col, pre_insert);
		break;
	}
	return result;
}

} // namespace duckdb

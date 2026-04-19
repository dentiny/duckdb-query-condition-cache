#include "logical_cache_recorder.hpp"

#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"

namespace duckdb {

LogicalCacheRecorder::LogicalCacheRecorder(idx_t table_oid_p, string canonical_key_p,
                                           unique_ptr<Expression> bound_predicate_p, idx_t rowid_column_index_p)
    : table_oid(table_oid_p), canonical_key(std::move(canonical_key_p)), rowid_column_index(rowid_column_index_p) {
	expressions.push_back(std::move(bound_predicate_p));
}

PhysicalOperator &LogicalCacheRecorder::CreatePlan(ClientContext &context, PhysicalPlanGenerator &planner) {
	auto &child_plan = planner.CreatePlan(*children[0]);
	auto bound_predicate = std::move(expressions[0]);
	auto &op = planner.Make<PhysicalCacheRecorder>(table_oid, canonical_key, std::move(bound_predicate),
	                                               rowid_column_index, child_plan.types, estimated_cardinality);
	op.children.push_back(child_plan);
	return op;
}

vector<ColumnBinding> LogicalCacheRecorder::GetColumnBindings() {
	return children[0]->GetColumnBindings();
}

void LogicalCacheRecorder::ResolveTypes() {
	types = children[0]->types;
}

string LogicalCacheRecorder::GetExtensionName() const {
	return "query_condition_cache_recorder";
}

void LogicalCacheRecorder::Serialize(Serializer &serializer) const {
	LogicalExtensionOperator::Serialize(serializer);
	serializer.WriteProperty(400, "table_oid", table_oid);
	serializer.WriteProperty(401, "canonical_key", canonical_key);
	serializer.WriteProperty(402, "rowid_column_index", rowid_column_index);
	serializer.WritePropertyWithDefault(403, "expressions", expressions);
}

namespace {

BoundStatement CacheRecorderBind(ClientContext &context, Binder &binder, OperatorExtensionInfo *info,
                                 SQLStatement &statement) {
	return BoundStatement();
}

} // namespace

CacheRecorderOperatorExtension::CacheRecorderOperatorExtension() {
	Bind = CacheRecorderBind;
}

string CacheRecorderOperatorExtension::GetName() {
	return "query_condition_cache_recorder";
}

unique_ptr<LogicalExtensionOperator> CacheRecorderOperatorExtension::Deserialize(Deserializer &deserializer) {
	auto oid = deserializer.ReadProperty<idx_t>(400, "table_oid");
	auto key = deserializer.ReadProperty<string>(401, "canonical_key");
	auto rowid_col = deserializer.ReadProperty<idx_t>(402, "rowid_column_index");
	auto exprs = deserializer.ReadPropertyWithDefault<vector<unique_ptr<Expression>>>(403, "expressions");

	unique_ptr<Expression> bound_predicate;
	if (!exprs.empty()) {
		bound_predicate = std::move(exprs[0]);
	} else {
		// Placeholder keeps the operator structurally intact for verification round-trips.
		bound_predicate = make_uniq<BoundReferenceExpression>(LogicalType {LogicalTypeId::BOOLEAN}, 0);
	}
	return make_uniq<LogicalCacheRecorder>(oid, std::move(key), std::move(bound_predicate), rowid_col);
}

} // namespace duckdb

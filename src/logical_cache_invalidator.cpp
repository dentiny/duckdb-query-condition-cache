#include "logical_cache_invalidator.hpp"

#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/binder.hpp"

namespace duckdb {

LogicalCacheInvalidator::LogicalCacheInvalidator(idx_t table_oid_p) : table_oid(table_oid_p) {
}

PhysicalOperator &LogicalCacheInvalidator::CreatePlan(ClientContext &context, PhysicalPlanGenerator &planner) {
	auto &child_plan = planner.CreatePlan(*children[0]);
	auto &op = planner.Make<PhysicalCacheInvalidator>(table_oid, child_plan.types, estimated_cardinality);
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
}

// --- CacheInvalidatorOperatorExtension ---

namespace {

BoundStatement CacheInvalidatorBind(ClientContext &context, Binder &binder, OperatorExtensionInfo *info,
                                    SQLStatement &statement) {
	// We don't bind any statements — return empty plan to signal "not handled"
	return BoundStatement();
}

} // namespace

CacheInvalidatorOperatorExtension::CacheInvalidatorOperatorExtension() {
	Bind = CacheInvalidatorBind;
}

string CacheInvalidatorOperatorExtension::GetName() {
	return "query_condition_cache";
}

unique_ptr<LogicalExtensionOperator> CacheInvalidatorOperatorExtension::Deserialize(Deserializer &deserializer) {
	auto oid = deserializer.ReadProperty<idx_t>(300, "table_oid");
	return make_uniq<LogicalCacheInvalidator>(oid);
}

} // namespace duckdb

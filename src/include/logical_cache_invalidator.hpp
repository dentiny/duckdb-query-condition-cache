#pragma once

#include "duckdb/planner/operator/logical_extension_operator.hpp"
#include "physical_cache_invalidator.hpp"

namespace duckdb {

//! Injected above a DML node so the invalidation runs from the plan. An optimizer-time hook would
//! miss EXECUTE, which replays a prepared plan without re-planning.
struct LogicalCacheInvalidator : public LogicalExtensionOperator {
	idx_t table_oid;

	explicit LogicalCacheInvalidator(idx_t table_oid_p);

	PhysicalOperator &CreatePlan(ClientContext &context, PhysicalPlanGenerator &planner) override;
	vector<ColumnBinding> GetColumnBindings() override;
	string GetExtensionName() const override;
	void Serialize(Serializer &serializer) const override;

protected:
	void ResolveTypes() override;
};

//! OperatorExtension for deserializing LogicalCacheInvalidator
class CacheInvalidatorOperatorExtension : public OperatorExtension {
public:
	CacheInvalidatorOperatorExtension();
	string GetName() override;
	unique_ptr<LogicalExtensionOperator> Deserialize(Deserializer &deserializer) override;
};

} // namespace duckdb

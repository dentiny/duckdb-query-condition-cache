#pragma once

#include "duckdb/planner/operator/logical_extension_operator.hpp"
#include "physical_cache_recorder.hpp"

namespace duckdb {

// Logical wrapper for PhysicalCacheRecorder. Carries the bound predicate already
// remapped to chunk column positions and the canonical key for the store.
struct LogicalCacheRecorder : public LogicalExtensionOperator {
	idx_t table_oid;
	string canonical_key;
	idx_t rowid_column_index;

	LogicalCacheRecorder(idx_t table_oid_p, string canonical_key_p, unique_ptr<Expression> bound_predicate_p,
	                     idx_t rowid_column_index_p);

	PhysicalOperator &CreatePlan(ClientContext &context, PhysicalPlanGenerator &planner) override;
	vector<ColumnBinding> GetColumnBindings() override;
	string GetExtensionName() const override;
	void Serialize(Serializer &serializer) const override;

protected:
	void ResolveTypes() override;
};

// Uses extension name "query_condition_cache_recorder" to avoid colliding with
// CacheInvalidatorOperatorExtension's "query_condition_cache".
class CacheRecorderOperatorExtension : public OperatorExtension {
public:
	CacheRecorderOperatorExtension();
	string GetName() override;
	unique_ptr<LogicalExtensionOperator> Deserialize(Deserializer &deserializer) override;
};

} // namespace duckdb

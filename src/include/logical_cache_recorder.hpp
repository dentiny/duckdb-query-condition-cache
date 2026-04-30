#pragma once

#include "duckdb/planner/operator/logical_extension_operator.hpp"
#include "physical_cache_recorder.hpp"

namespace duckdb {

struct LogicalCacheRecorder : public LogicalExtensionOperator {
	idx_t table_oid;
	string table_catalog;
	string table_schema;
	string table_name;
	string canonical_key;
	idx_t rowid_column_index;
	shared_ptr<ConditionCacheEntry> cache_entry;
	shared_ptr<ConditionCacheEntry> metadata_entry;

	LogicalCacheRecorder(idx_t table_oid_p, string canonical_key_p, unique_ptr<Expression> bound_predicate_p,
	                     idx_t rowid_column_index_p, string table_catalog_p = string(),
	                     string table_schema_p = string(), string table_name_p = string(),
	                     unique_ptr<Expression> backfill_predicate_p = nullptr,
	                     shared_ptr<ConditionCacheEntry> cache_entry_p = nullptr,
	                     shared_ptr<ConditionCacheEntry> metadata_entry_p = nullptr);

	PhysicalOperator &CreatePlan(ClientContext &context, PhysicalPlanGenerator &planner) override;
	vector<ColumnBinding> GetColumnBindings() override;
	string GetExtensionName() const override;
	void Serialize(Serializer &serializer) const override;

protected:
	void ResolveTypes() override;
};

class CacheRecorderOperatorExtension : public OperatorExtension {
public:
	CacheRecorderOperatorExtension();
	string GetName() override;
	unique_ptr<LogicalExtensionOperator> Deserialize(Deserializer &deserializer) override;
};

} // namespace duckdb

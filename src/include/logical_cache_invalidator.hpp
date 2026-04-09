#pragma once

#include "duckdb/planner/operator/logical_extension_operator.hpp"
#include "physical_cache_invalidator.hpp"

namespace duckdb {

struct LogicalCacheInvalidator : public LogicalExtensionOperator {
	idx_t table_oid;
	CacheInvalidatorMode mode;
	idx_t row_id_column_index;  // for INVALIDATE mode
	idx_t pre_insert_row_count; // for INSERT/MERGE modes

	// For DELETE/UPDATE: pass the row_id expression to be resolved during column binding.
	LogicalCacheInvalidator(idx_t table_oid_p, unique_ptr<Expression> row_id_expr_p);

	// For INSERT: count rows and compute affected range.
	LogicalCacheInvalidator(idx_t table_oid_p, idx_t pre_insert_row_count_p);

	// For MERGE: hybrid — track row IDs (at row_id_column_index) for matched rows
	// and count unmatched rows for the insert range.
	LogicalCacheInvalidator(idx_t table_oid_p, idx_t row_id_column_index_p, idx_t pre_insert_row_count_p);

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

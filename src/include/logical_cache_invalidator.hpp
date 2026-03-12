#pragma once

#include "duckdb/planner/operator/logical_extension_operator.hpp"
#include "physical_cache_invalidator.hpp"

namespace duckdb {

struct LogicalCacheInvalidator : public LogicalExtensionOperator {
	idx_t table_oid;
	CacheInvalidatorMode mode;
	idx_t row_id_column_index;  // for ROW_ID mode
	idx_t pre_insert_row_count; // for INSERT/MERGE modes

	// For DELETE/UPDATE: pass the row_id expression to be resolved during column binding.
	LogicalCacheInvalidator(idx_t table_oid, unique_ptr<Expression> row_id_expr);

	// For INSERT: count rows and compute affected range.
	LogicalCacheInvalidator(idx_t table_oid, idx_t pre_insert_row_count);

	// For MERGE: hybrid — track row IDs (at row_id_column_index) for matched rows
	// and count unmatched rows for the insert range.
	LogicalCacheInvalidator(idx_t table_oid, idx_t row_id_column_index, idx_t pre_insert_row_count);

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
	std::string GetName() override;
	unique_ptr<LogicalExtensionOperator> Deserialize(Deserializer &deserializer) override;
};

} // namespace duckdb

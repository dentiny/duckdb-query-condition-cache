#pragma once

#include "duckdb/planner/operator/logical_extension_operator.hpp"
#include "physical_cache_building_filter.hpp"

namespace duckdb {

// Logical wrapper for the fused-build filter. It expects its child to expose a
// hidden ROW_ID column at row_id_column_index and hides that column from parents.
struct LogicalCacheBuildingFilter : public LogicalExtensionOperator {
	explicit LogicalCacheBuildingFilter(vector<unique_ptr<Expression>> filter_expressions_p,
	                                    unique_ptr<Expression> row_id_reference_p, idx_t row_id_column_index_p,
	                                    bool hide_row_id_column_p,
	                                    shared_ptr<ConditionCacheEntry> cache_entry_p = nullptr);

	idx_t row_id_column_index;
	bool hide_row_id_column;
	idx_t filter_expression_count;
	shared_ptr<ConditionCacheEntry> cache_entry;

	PhysicalOperator &CreatePlan(ClientContext &context, PhysicalPlanGenerator &planner) override;
	vector<ColumnBinding> GetColumnBindings() override;
	string GetExtensionName() const override;
	void Serialize(Serializer &serializer) const override;
	bool SupportSerialization() const override {
		return false;
	}

protected:
	void ResolveTypes() override;

private:
	bool TryResolveRowIdColumnIndex(idx_t &resolved_index) const;
};

} // namespace duckdb

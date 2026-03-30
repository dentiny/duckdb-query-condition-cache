#pragma once

#include "duckdb/execution/physical_operator.hpp"

namespace duckdb {

enum class CacheInvalidatorMode : uint8_t {
	// UPDATE-style invalidation: observe row IDs at row_id_column_index
	ROW_ID,
	// DELETE/TRUNCATE: clear all cache entries for the table
	CLEAR_TABLE,
	// INSERT: count rows and compute affected range from pre_insert_row_count
	INSERT,
	// MERGE: hybrid — track row IDs for matched rows (UPDATE/DELETE) and count
	// unmatched rows (INSERT) to compute the insert range
	MERGE
};

struct CacheInvalidatorGlobalState : public GlobalOperatorState {
	mutex lock;
	unordered_set<idx_t> affected_row_groups;
	idx_t inserted_row_count = 0; // used in INSERT and MERGE modes
};

class PhysicalCacheInvalidator : public PhysicalOperator {
public:
	PhysicalCacheInvalidator(PhysicalPlan &physical_plan, idx_t table_oid, CacheInvalidatorMode mode,
	                         idx_t row_id_column_index, idx_t pre_insert_row_count, vector<LogicalType> types,
	                         idx_t estimated_cardinality);

	idx_t table_oid;
	CacheInvalidatorMode mode;
	idx_t row_id_column_index;
	idx_t pre_insert_row_count;

	unique_ptr<GlobalOperatorState> GetGlobalOperatorState(ClientContext &context) const override;
	OperatorResultType Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
	                           GlobalOperatorState &gstate, OperatorState &state) const override;
	OperatorFinalResultType OperatorFinalize(Pipeline &pipeline, Event &event, ClientContext &context,
	                                         OperatorFinalizeInput &input) const override;
	bool RequiresOperatorFinalize() const override;
	bool ParallelOperator() const override;
	string GetName() const override;
	InsertionOrderPreservingMap<string> ParamsToString() const override;
};

} // namespace duckdb

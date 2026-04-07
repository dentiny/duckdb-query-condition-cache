#pragma once

#include "concurrency/annotated_mutex.hpp"
#include "concurrency/thread_annotation.hpp"

#include "duckdb/execution/physical_operator.hpp"

namespace duckdb {

enum class CacheInvalidatorMode : uint8_t {
	// DELETE/UPDATE: observe row IDs at row_id_column_index to find affected row groups.
	// TODO: Indexed column UPDATE does DELETE + LocalAppend internally. New rows flushed
	// to an existing row group may have stale cache. Consider using MERGE mode for
	// indexed UPDATE in CacheInvalidationOptimizer.
	INVALIDATE,
	// INSERT: count rows and compute affected range from pre_insert_row_count
	INSERT,
	// MERGE: hybrid -- track row IDs for matched rows and count
	// unmatched rows (INSERT) to compute the insert range
	MERGE
};

// Shared state across parallel Execute() calls. Accumulates affected row groups
// and inserted row counts under a mutex.
struct CacheInvalidatorGlobalState : public GlobalOperatorState {
	concurrency::mutex lock;
	unordered_set<idx_t> affected_row_groups DUCKDB_GUARDED_BY(lock);
	idx_t inserted_row_count DUCKDB_GUARDED_BY(lock) = 0;
};

// Passthrough physical operator injected into DML plans (INSERT/DELETE/UPDATE/MERGE).
// Observes the data stream to collect affected row group indices, then invalidates
// those row groups from the condition cache in OperatorFinalize.
// Mode is set by CacheInvalidationOptimizer based on the DML type.
class PhysicalCacheInvalidator : public PhysicalOperator {
public:
	PhysicalCacheInvalidator(PhysicalPlan &physical_plan, idx_t table_oid_p, CacheInvalidatorMode mode_p,
	                         idx_t row_id_column_index_p, idx_t pre_insert_row_count_p, vector<LogicalType> types,
	                         idx_t estimated_cardinality);

	idx_t table_oid;
	CacheInvalidatorMode mode;
	// Column index within input chunks that contains row IDs (INVALIDATE/MERGE modes only)
	idx_t row_id_column_index;
	// Number of rows in the table before this DML (INSERT/MERGE modes only)
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

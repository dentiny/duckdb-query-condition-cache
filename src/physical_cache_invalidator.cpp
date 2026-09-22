#include "physical_cache_invalidator.hpp"

#include "query_condition_cache_state.hpp"

#include "duckdb/common/types/data_chunk.hpp"

namespace duckdb {

PhysicalCacheInvalidator::PhysicalCacheInvalidator(PhysicalPlan &physical_plan, idx_t table_oid_p,
                                                   vector<LogicalType> types, idx_t estimated_cardinality)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, std::move(types), estimated_cardinality),
      table_oid(table_oid_p) {
}

OperatorResultType PhysicalCacheInvalidator::Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
                                                     GlobalOperatorState &gstate, OperatorState &state) const {
	chunk.Reference(input);
	return OperatorResultType::NEED_MORE_INPUT;
}

// Runs before the transaction commits: a statement that rolls back drops entries for nothing,
// which costs a rebuild. Keeping a stale entry would return wrong rows.
OperatorFinalResultType PhysicalCacheInvalidator::OperatorFinalize(Pipeline &pipeline, Event &event,
                                                                   ClientContext &context,
                                                                   OperatorFinalizeInput &input) const {
	auto store = ConditionCacheStore::GetOrCreate(context);
	store->RemoveAllEntriesForTable(context, table_oid);
	return OperatorFinalResultType::FINISHED;
}

bool PhysicalCacheInvalidator::RequiresOperatorFinalize() const {
	return true;
}

bool PhysicalCacheInvalidator::ParallelOperator() const {
	return true;
}

string PhysicalCacheInvalidator::GetName() const {
	return "CACHE_INVALIDATOR";
}

InsertionOrderPreservingMap<string> PhysicalCacheInvalidator::ParamsToString() const {
	InsertionOrderPreservingMap<string> result;
	result["Table OID"] = to_string(table_oid);
	return result;
}

} // namespace duckdb

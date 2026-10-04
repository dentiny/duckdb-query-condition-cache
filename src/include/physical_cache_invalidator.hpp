#pragma once

#include "duckdb/execution/physical_operator.hpp"

namespace duckdb {

//! Pass-through operator injected into DML plans. Its only job is to run when the DML runs, so
//! OperatorFinalize can drop every cache entry for the table.
class PhysicalCacheInvalidator : public PhysicalOperator {
public:
	PhysicalCacheInvalidator(PhysicalPlan &physical_plan, idx_t table_oid_p, vector<LogicalType> types,
	                         idx_t estimated_cardinality);

	idx_t table_oid;

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

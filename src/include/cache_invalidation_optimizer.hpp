#pragma once

#include "duckdb/optimizer/optimizer_extension.hpp"

namespace duckdb {

class CacheInvalidationOptimizer : public OptimizerExtension {
public:
	CacheInvalidationOptimizer();

	//! Post-optimize: walk the plan and inject cache invalidation operators for DML statements
	static void OptimizeFunction(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan);

private:
	//! Walk plan to find DML operators (INSERT/UPDATE/DELETE/MERGE) and inject invalidation
	static void WalkPlanForDML(ClientContext &context, unique_ptr<LogicalOperator> &op);
};

} // namespace duckdb

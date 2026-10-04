#include "cache_invalidation_optimizer.hpp"

#include "logical_cache_invalidator.hpp"

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/enums/logical_operator_type.hpp"
#include "duckdb/planner/operator/logical_delete.hpp"
#include "duckdb/planner/operator/logical_insert.hpp"
#include "duckdb/planner/operator/logical_merge_into.hpp"
#include "duckdb/planner/operator/logical_update.hpp"

namespace duckdb {

CacheInvalidationOptimizer::CacheInvalidationOptimizer() {
	optimize_function = OptimizeFunction;
}

namespace {

void InjectInvalidator(LogicalOperator &dml, idx_t table_oid) {
	auto invalidator = make_uniq<LogicalCacheInvalidator>(table_oid);
	invalidator->children = std::move(dml.children);
	dml.children.clear();
	dml.children.push_back(std::move(invalidator));
}

} // namespace

void CacheInvalidationOptimizer::WalkPlanForDML(ClientContext &context, unique_ptr<LogicalOperator> &op) {
	for (auto &child : op->children) {
		WalkPlanForDML(context, child);
	}

	switch (op->type) {
	case LogicalOperatorType::LOGICAL_DELETE:
		InjectInvalidator(*op, op->Cast<LogicalDelete>().table.oid);
		break;
	case LogicalOperatorType::LOGICAL_UPDATE:
		InjectInvalidator(*op, op->Cast<LogicalUpdate>().table.oid);
		break;
	case LogicalOperatorType::LOGICAL_INSERT:
		InjectInvalidator(*op, op->Cast<LogicalInsert>().table.oid);
		break;
	case LogicalOperatorType::LOGICAL_MERGE_INTO:
		InjectInvalidator(*op, op->Cast<LogicalMergeInto>().table.oid);
		break;
	default:
		break;
	}
}

void CacheInvalidationOptimizer::OptimizeFunction(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan) {
	WalkPlanForDML(input.context, plan);
}

} // namespace duckdb

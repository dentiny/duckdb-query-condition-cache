#include "cache_invalidation_optimizer.hpp"

#include "logical_cache_invalidator.hpp"
#include "query_condition_cache_functions.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/common/enums/logical_operator_type.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/operator/logical_delete.hpp"
#include "duckdb/planner/operator/logical_insert.hpp"
#include "duckdb/planner/operator/logical_merge_into.hpp"
#include "duckdb/planner/operator/logical_update.hpp"

namespace duckdb {

CacheInvalidationOptimizer::CacheInvalidationOptimizer() {
	optimize_function = OptimizeFunction;
}

void CacheInvalidationOptimizer::WalkPlanForDML(ClientContext &context, unique_ptr<LogicalOperator> &op) {
	// Recurse into children first
	for (auto &child : op->children) {
		WalkPlanForDML(context, child);
	}

	switch (op->type) {
	case LogicalOperatorType::LOGICAL_DELETE: {
		auto &del = op->Cast<LogicalDelete>();
		auto table_oid = del.table.oid;

		// Copy the row_id expression; it will be resolved during column binding resolution
		auto row_id_expr = del.expressions[0]->Copy();

		auto invalidator = make_uniq<LogicalCacheInvalidator>(table_oid, std::move(row_id_expr));
		invalidator->children = std::move(del.children);
		del.children.clear();
		del.children.push_back(std::move(invalidator));
		break;
	}
	case LogicalOperatorType::LOGICAL_UPDATE: {
		auto &upd = op->Cast<LogicalUpdate>();
		auto table_oid = upd.table.oid;

		// row_id is always the last column in an UPDATE's child output
		auto row_id_col_idx = upd.children[0]->GetColumnBindings().size() - 1;
		auto row_id_expr = make_uniq<BoundReferenceExpression>(LogicalType::BIGINT, row_id_col_idx);

		auto invalidator = make_uniq<LogicalCacheInvalidator>(table_oid, std::move(row_id_expr));
		invalidator->children = std::move(upd.children);
		upd.children.clear();
		upd.children.push_back(std::move(invalidator));
		break;
	}
	case LogicalOperatorType::LOGICAL_INSERT: {
		auto &ins = op->Cast<LogicalInsert>();
		auto table_oid = ins.table.oid;

		auto &duck_table = ins.table.Cast<DuckTableEntry>();
		auto pre_insert_rows = duck_table.GetStorage().GetTotalRows();

		auto invalidator = make_uniq<LogicalCacheInvalidator>(table_oid, pre_insert_rows);
		invalidator->children = std::move(ins.children);
		ins.children.clear();
		ins.children.push_back(std::move(invalidator));
		break;
	}
	case LogicalOperatorType::LOGICAL_MERGE_INTO: {
		auto &merge = op->Cast<LogicalMergeInto>();
		auto table_oid = merge.table.oid;
		auto &duck_table = merge.table.Cast<DuckTableEntry>();
		auto pre_insert_rows = duck_table.GetStorage().GetTotalRows();

		auto row_id_col = merge.row_id_start;

		auto invalidator = make_uniq<LogicalCacheInvalidator>(table_oid, row_id_col, pre_insert_rows);
		invalidator->children = std::move(merge.children);
		merge.children.clear();
		merge.children.push_back(std::move(invalidator));
		break;
	}
	default:
		break;
	}
}

void CacheInvalidationOptimizer::OptimizeFunction(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan) {
	WalkPlanForDML(input.context, plan);
}

} // namespace duckdb

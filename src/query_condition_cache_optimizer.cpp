#include "query_condition_cache_optimizer.hpp"
#include "query_condition_cache_filter.hpp"
#include "query_condition_cache_functions.hpp"
#include "query_condition_cache_state.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/algorithm.hpp"
#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression/bound_between_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/expression_binder/check_binder.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_get.hpp"

namespace duckdb {

thread_local unordered_map<idx_t, pair<CacheKey, shared_ptr<ConditionCacheEntry>>> tl_cache_apply_pending;

QueryConditionCacheOptimizer::QueryConditionCacheOptimizer() {
	pre_optimize_function = PreOptimizeFunction;
	optimize_function = OptimizeFunction;
}

// ============================================================================
// Setting check
// ============================================================================

bool QueryConditionCacheOptimizer::IsSettingEnabled(ClientContext &context) {
	Value val;
	auto result = context.TryGetCurrentSetting("use_query_condition_cache", val);
	if (!result) {
		return false;
	}
	return val.GetValue<bool>();
}

// ============================================================================
// Pre-optimize: runs BEFORE built-in passes (including FilterPushdown).
// The full WHERE clause is still in LogicalFilter.expressions, so we can
// compute a canonical key that covers the entire predicate.
// On cache miss, builds cache inline so the first query benefits immediately.
// ============================================================================

void QueryConditionCacheOptimizer::PreOptimizeFunction(OptimizerExtensionInput &input,
                                                       unique_ptr<LogicalOperator> &plan) {
	if (!IsSettingEnabled(input.context)) {
		return;
	}
	tl_cache_apply_pending.clear();
	unordered_map<idx_t, LogicalGet *> gets;
	CollectGets(plan, gets, false);
	ProcessFilters(input.context, plan, gets, false);
}

void QueryConditionCacheOptimizer::CollectGets(unique_ptr<LogicalOperator> &plan,
                                               unordered_map<idx_t, LogicalGet *> &gets, bool inside_dml) {
	bool is_dml =
	    plan->type == LogicalOperatorType::LOGICAL_DELETE || plan->type == LogicalOperatorType::LOGICAL_UPDATE ||
	    plan->type == LogicalOperatorType::LOGICAL_INSERT || plan->type == LogicalOperatorType::LOGICAL_MERGE_INTO;
	bool child_inside_dml = inside_dml || is_dml;

	if (plan->type == LogicalOperatorType::LOGICAL_GET && !inside_dml) {
		auto &get = plan->Cast<LogicalGet>();
		gets[get.table_index] = &get;
	}

	for (auto &child : plan->children) {
		CollectGets(child, gets, child_inside_dml);
	}
}

// Check if an expression will be natively pushed down by DuckDB's FilterPushdown optimizer,
// meaning the condition cache provides no additional benefit.
// Mirrors the logic in FilterCombiner::AddFilter + TryPushdownExpression.
static bool IsTriviallyPushable(const Expression &expr) {
	// Comparisons with a constant side: col op const, const op col
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_COMPARISON) {
		auto &comp = expr.Cast<BoundComparisonExpression>();
		return comp.left->IsFoldable() || comp.right->IsFoldable();
	}
	// BETWEEN with constant bounds
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_BETWEEN) {
		auto &between = expr.Cast<BoundBetweenExpression>();
		return between.lower->IsFoldable() && between.upper->IsFoldable();
	}
	// IS NULL / IS NOT NULL
	if (expr.type == ExpressionType::OPERATOR_IS_NULL || expr.type == ExpressionType::OPERATOR_IS_NOT_NULL) {
		return true;
	}
	// IN with constant children: col IN (const, const, ...)
	if (expr.type == ExpressionType::COMPARE_IN && expr.GetExpressionClass() == ExpressionClass::BOUND_OPERATOR) {
		auto &op = expr.Cast<BoundOperatorExpression>();
		if (op.children.size() > 1 && op.children[0]->GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF) {
			bool all_const = true;
			for (idx_t i = 1; i < op.children.size(); i++) {
				if (op.children[i]->GetExpressionType() != ExpressionType::VALUE_CONSTANT) {
					all_const = false;
					break;
				}
			}
			return all_const;
		}
	}
	// LIKE with constant pattern: col LIKE 'literal' (function name "~~")
	// prefix(col, 'literal')
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_FUNCTION) {
		auto &func = expr.Cast<BoundFunctionExpression>();
		if ((func.function.name == "~~" || func.function.name == "prefix") && func.children.size() == 2 &&
		    func.children[0]->GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF &&
		    func.children[1]->GetExpressionType() == ExpressionType::VALUE_CONSTANT) {
			return true;
		}
	}
	// OR of simple comparisons: col = 1 OR col = 2 OR ...
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_CONJUNCTION) {
		auto &conj = expr.Cast<BoundConjunctionExpression>();
		if (conj.GetExpressionType() == ExpressionType::CONJUNCTION_OR) {
			for (auto &child : conj.children) {
				if (child->GetExpressionClass() != ExpressionClass::BOUND_COMPARISON) {
					return false;
				}
				auto &comp = child->Cast<BoundComparisonExpression>();
				bool has_col_and_const = (comp.left->GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF &&
				                          comp.right->GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) ||
				                         (comp.left->GetExpressionClass() == ExpressionClass::BOUND_CONSTANT &&
				                          comp.right->GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF);
				if (!has_col_and_const) {
					return false;
				}
			}
			return !conj.children.empty();
		}
	}
	return false;
}

void QueryConditionCacheOptimizer::ExtractBindings(Expression &expr, unordered_set<idx_t> &table_bindings) {
	if (expr.type == ExpressionType::BOUND_COLUMN_REF) {
		auto &colref = expr.Cast<BoundColumnRefExpression>();
		table_bindings.insert(colref.binding.table_index);
	}
	ExpressionIterator::EnumerateChildren(expr, [&](Expression &child) { ExtractBindings(child, table_bindings); });
}

void QueryConditionCacheOptimizer::ProcessFilters(ClientContext &context, unique_ptr<LogicalOperator> &plan,
                                                  unordered_map<idx_t, LogicalGet *> &gets, bool inside_dml) {
	bool is_dml =
	    plan->type == LogicalOperatorType::LOGICAL_DELETE || plan->type == LogicalOperatorType::LOGICAL_UPDATE ||
	    plan->type == LogicalOperatorType::LOGICAL_INSERT || plan->type == LogicalOperatorType::LOGICAL_MERGE_INTO;
	bool child_inside_dml = inside_dml || is_dml;

	if (!inside_dml && plan->type == LogicalOperatorType::LOGICAL_FILTER) {
		auto &filter = plan->Cast<LogicalFilter>();
		unordered_map<idx_t, vector<unique_ptr<Expression>>> table_expressions;

		for (auto &expr : filter.expressions) {
			unordered_set<idx_t> table_bindings;
			ExtractBindings(*expr, table_bindings);

			// Only process expressions that belong to exactly one table
			if (table_bindings.size() == 1) {
				idx_t table_index = *table_bindings.begin();
				if (gets.find(table_index) != gets.end()) {
					table_expressions[table_index].push_back(expr->Copy());
				}
			}
		}

		for (auto &pair : table_expressions) {
			idx_t table_index = pair.first;
			auto &expressions = pair.second;
			auto get = gets[table_index];

			// Skip cache processing if all predicates are trivially pushable by DuckDB's
			// native FilterPushdown (comparisons with constants, BETWEEN, IS NULL/NOT NULL).
			// The cache provides no benefit for these — DuckDB already handles them efficiently.
			bool all_trivially_pushable = true;
			for (auto &e : expressions) {
				if (!IsTriviallyPushable(*e)) {
					all_trivially_pushable = false;
					break;
				}
			}
			if (all_trivially_pushable) {
				continue;
			}

			auto table = get->GetTable();
			if (!table) {
				continue;
			}

			auto key = ComputePredicateKey(table->oid, expressions);
			auto store = ConditionCacheStore::GetOrCreate(context);
			auto entry = store->Lookup(context, key);

			if (!entry) {
				entry = BuildCacheForPredicate(context, expressions, *get);
				if (entry) {
					store->Upsert(context, key, entry);
				}
			}
			if (entry) {
				tl_cache_apply_pending[table_index] = make_pair(key, entry);
			}
		}
	}

	for (auto &child : plan->children) {
		ProcessFilters(context, child, gets, child_inside_dml);
	}
}
CacheKey QueryConditionCacheOptimizer::ComputePredicateKey(idx_t table_oid,
                                                           const vector<unique_ptr<Expression>> &expressions) {
	vector<string> parts;
	parts.reserve(expressions.size());
	for (auto &expr : expressions) {
		parts.push_back(expr->ToString());
	}
	std::sort(parts.begin(), parts.end());
	return CacheKey {table_oid, StringUtil::Join(parts, ";")};
}

shared_ptr<ConditionCacheEntry> QueryConditionCacheOptimizer::BuildCacheForPredicate(
    ClientContext &context, const vector<unique_ptr<Expression>> &expressions, LogicalGet &get) {
	auto table_ptr = get.GetTable();
	if (!table_ptr) {
		return nullptr;
	}

	// We need a DuckTableEntry to use BuildCacheEntry
	auto &table_entry = table_ptr->Cast<DuckTableEntry>();

	// Reconstruct predicate SQL from the bound expressions.
	// Join multiple filter expressions with AND.
	vector<string> parts;
	parts.reserve(expressions.size());
	for (auto &expr : expressions) {
		parts.push_back(expr->ToString());
	}
	string predicate_sql = StringUtil::Join(parts, " AND ");

	// Parse, bind, and build cache entry (same flow as condition_cache_build table function)
	auto parsed_exprs = Parser::ParseExpressionList(predicate_sql);
	if (parsed_exprs.empty()) {
		return nullptr;
	}

	auto binder = Binder::CreateBinder(context);
	physical_index_set_t bound_columns;
	CheckBinder check_binder(*binder, context, table_entry.name, table_entry.GetColumns(), bound_columns);
	auto bound_expr = check_binder.Bind(parsed_exprs[0]);

	// CheckBinder sets target_type = INTEGER, re-cast to BOOLEAN
	bound_expr =
	    BoundCastExpression::AddCastToType(context, std::move(bound_expr), LogicalType {LogicalTypeId::BOOLEAN});

	return BuildCacheEntry(context, table_entry, *bound_expr);
}

// ============================================================================
// InjectCacheExpression: add __condition_cache_filter on ROW_ID
// ============================================================================

void QueryConditionCacheOptimizer::InjectCacheExpression(LogicalGet &get, const CacheKey &key,
                                                         const shared_ptr<ConditionCacheEntry> &entry) {
	// Ensure ROW_ID column is in the scan so the filter can access it
	bool has_rowid = false;
	idx_t row_id_col_idx = 0;
	for (idx_t i = 0; i < get.GetColumnIds().size(); ++i) {
		if (get.GetColumnIds()[i].IsRowIdColumn()) {
			has_rowid = true;
			row_id_col_idx = i;
			break;
		}
	}
	if (!has_rowid) {
		if (get.projection_ids.empty()) {
			for (idx_t i = 0; i < get.GetColumnIds().size(); ++i) {
				get.projection_ids.push_back(i);
			}
		}
		get.AddColumnId(COLUMN_IDENTIFIER_ROW_ID);
		row_id_col_idx = get.GetColumnIds().size() - 1;
	}

	ScalarFunction func("__condition_cache_filter", {LogicalType {LogicalTypeId::BIGINT}},
	                    LogicalType {LogicalTypeId::BOOLEAN}, ConditionCacheFilterFn, ConditionCacheFilterBind, nullptr,
	                    nullptr, ConditionCacheFilterInit);

	auto bind_data = make_uniq<ConditionCacheFilterBindData>(entry);

	vector<unique_ptr<Expression>> children;
	children.push_back(make_uniq<BoundReferenceExpression>(LogicalType::BIGINT, 0));

	auto func_expr = make_uniq<BoundFunctionExpression>(LogicalType {LogicalTypeId::BOOLEAN}, func, std::move(children),
	                                                    std::move(bind_data));

	auto cache_filter = make_uniq<CacheExpressionFilter>(std::move(func_expr), key, entry);
	get.table_filters.PushFilter(get.GetColumnIds()[row_id_col_idx], std::move(cache_filter));
}

void QueryConditionCacheOptimizer::OptimizeFunction(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan) {
	if (tl_cache_apply_pending.empty()) {
		return;
	}
	PostOptimizeWalk(plan);
	tl_cache_apply_pending.clear();
}

void QueryConditionCacheOptimizer::PostOptimizeWalk(unique_ptr<LogicalOperator> &plan) {
	for (auto &child : plan->children) {
		PostOptimizeWalk(child);
	}

	if (plan->type != LogicalOperatorType::LOGICAL_GET) {
		return;
	}

	auto &get = plan->Cast<LogicalGet>();
	auto it = tl_cache_apply_pending.find(get.table_index);
	if (it == tl_cache_apply_pending.end()) {
		return;
	}

	InjectCacheExpression(get, it->second.first, it->second.second);
	tl_cache_apply_pending.erase(it);
}

} // namespace duckdb

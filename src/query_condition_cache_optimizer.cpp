#include "query_condition_cache_optimizer.hpp"

#include "query_condition_cache_functions.hpp"
#include "query_condition_cache_state.hpp"

#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/common/algorithm.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression_binder/check_binder.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_get.hpp"

namespace duckdb {

QueryConditionCacheOptimizer::QueryConditionCacheOptimizer() {
	pre_optimize_function = PreOptimizeFunction;
	optimize_function = OptimizeFunction;
}

bool QueryConditionCacheOptimizer::IsSettingEnabled(ClientContext &context) {
	Value val;
	auto result = context.TryGetCurrentSetting("use_query_condition_cache", val);
	if (!result) {
		return false;
	}
	return val.GetValue<bool>();
}

// Pre-optimize runs BEFORE built-in passes (including FilterPushdown).
// The full WHERE clause is still in LogicalFilter.expressions, so we can
// compute a canonical key that covers the entire predicate.
void QueryConditionCacheOptimizer::PreOptimizeFunction(OptimizerExtensionInput &input,
                                                       unique_ptr<LogicalOperator> &plan) {
	if (!IsSettingEnabled(input.context)) {
		return;
	}
	auto query_state = input.context.registered_state->GetOrCreate<CacheOptimizerQueryState>("qcc_optimizer_state");
	query_state->cache_apply_pending.clear();
	try {
		PreOptimizeWalk(input.context, plan, /*inside_dml=*/false, *query_state);
	} catch (...) {
		// If anything goes wrong, clear pending entries and let the query
		// proceed without cache optimization
		query_state->cache_apply_pending.clear();
	}
}

void QueryConditionCacheOptimizer::PreOptimizeWalk(ClientContext &context, unique_ptr<LogicalOperator> &plan,
                                                   bool inside_dml, CacheOptimizerQueryState &state) {
	// Skip cache building inside DML subplans
	bool is_dml =
	    plan->type == LogicalOperatorType::LOGICAL_DELETE || plan->type == LogicalOperatorType::LOGICAL_UPDATE ||
	    plan->type == LogicalOperatorType::LOGICAL_INSERT || plan->type == LogicalOperatorType::LOGICAL_MERGE_INTO;
	bool child_inside_dml = inside_dml || is_dml;

	for (auto &child : plan->children) {
		PreOptimizeWalk(context, child, child_inside_dml, state);
	}

	if (inside_dml) {
		return;
	}

	// Only handle direct table scans with a filter (LogicalFilter -> LogicalGet).
	// Joins, subqueries, and other patterns are not supported currently.
	if (plan->type != LogicalOperatorType::LOGICAL_FILTER || plan->children.empty()) {
		return;
	}
	if (plan->children[0]->type != LogicalOperatorType::LOGICAL_GET) {
		return;
	}

	auto &filter = plan->Cast<LogicalFilter>();
	auto &get = plan->children[0]->Cast<LogicalGet>();
	auto table = get.GetTable();
	if (!table) {
		return; // not a DuckDB table (e.g. system table, external)
	}
	if (filter.expressions.empty()) {
		return;
	}

	auto &duck_table = table->Cast<DuckTableEntry>();
	auto &storage = duck_table.GetStorage();

	// Skip caching for tables with indexes — index scans are typically more efficient
	if (storage.HasIndexes()) {
		return;
	}

	// Compute canonical key using parse+bind normalization (same format as condition_cache_build/info)
	auto key = ComputePredicateKey(context, table->oid, filter.expressions, get);
	if (key.filter_key.empty()) {
		return;
	}

	auto store = ConditionCacheStore::GetOrCreate(context);
	auto entry = store->Lookup(context, key);

	if (!entry) {
		// Cache miss: build cache inline
		entry = BuildCacheForPredicate(context, filter.expressions, get);
		if (entry) {
			store->Upsert(context, key, entry);
		}
	}

	if (entry) {
		state.cache_apply_pending[get.table_index] = std::move(entry);
	}
}

CacheKey QueryConditionCacheOptimizer::ComputePredicateKey(ClientContext &context, idx_t table_oid,
                                                           const vector<unique_ptr<Expression>> &expressions,
                                                           LogicalGet &get) {
	auto table_ptr = get.GetTable();
	if (!table_ptr) {
		return CacheKey {table_oid, ""};
	}
	auto &table_entry = table_ptr->Cast<DuckTableEntry>();

	// Reconstruct SQL from expressions, sort for canonical ordering
	vector<string> parts;
	parts.reserve(expressions.size());
	for (const auto &expr : expressions) {
		parts.push_back(expr->ToString());
	}
	sort(parts.begin(), parts.end());
	string predicate_sql = StringUtil::Join(parts, " AND ");

	// Parse and bind to produce the same normalized key as condition_cache_build/info
	auto parsed_exprs = Parser::ParseExpressionList(predicate_sql);
	if (parsed_exprs.empty()) {
		return CacheKey {table_oid, ""};
	}

	auto binder = Binder::CreateBinder(context);
	physical_index_set_t bound_columns;
	CheckBinder check_binder(*binder, context, table_entry.name, table_entry.GetColumns(), bound_columns);
	auto bound_expr = check_binder.Bind(parsed_exprs[0]);
	NormalizeExpressionForKey(*bound_expr);

	return CacheKey {table_oid, bound_expr->ToString()};
}

shared_ptr<ConditionCacheEntry> QueryConditionCacheOptimizer::BuildCacheForPredicate(
    ClientContext &context, const vector<unique_ptr<Expression>> &expressions, LogicalGet &get) {
	auto table_ptr = get.GetTable();
	if (!table_ptr) {
		return nullptr;
	}

	auto &table_entry = table_ptr->Cast<DuckTableEntry>();

	// Reconstruct predicate SQL from the bound expressions
	vector<string> parts;
	parts.reserve(expressions.size());
	for (const auto &expr : expressions) {
		parts.push_back(expr->ToString());
	}
	string predicate_sql = StringUtil::Join(parts, " AND ");

	// Parse, bind, and build cache entry (same flow as condition_cache_build)
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

// TODO: Implement PostOptimizeWalk + InjectCacheFilter to inject cache filters
// into LogicalGet nodes using cache_apply_pending entries.
void QueryConditionCacheOptimizer::OptimizeFunction(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan) {
}

} // namespace duckdb

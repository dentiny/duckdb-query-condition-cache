#pragma once

#include "query_condition_cache_state.hpp"

#include "duckdb/optimizer/optimizer_extension.hpp"

namespace duckdb {

class QueryConditionCacheOptimizer : public OptimizerExtension {
public:
	QueryConditionCacheOptimizer();

	//! Pre-optimize: compute canonical predicate keys before FilterPushdown splits the WHERE clause.
	//! On cache miss, builds cache inline so the first query benefits immediately.
	static void PreOptimizeFunction(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan);

	//! Check if use_query_condition_cache setting is enabled
	static bool IsSettingEnabled(ClientContext &context);

private:
	//! Walk plan pre-pushdown: find LogicalFilter→LogicalGet, compute key, lookup/build cache
	static void PreOptimizeWalk(ClientContext &context, unique_ptr<LogicalOperator> &plan, bool inside_dml);

	//! Compute a canonical predicate key from filter expressions (sorted, joined with ";")
	static CacheKey ComputePredicateKey(idx_t table_oid, const vector<unique_ptr<Expression>> &expressions);

	//! Build cache entry for a predicate on a table, store in cache store, and return it
	static shared_ptr<ConditionCacheEntry>
	BuildCacheForPredicate(ClientContext &context, const vector<unique_ptr<Expression>> &expressions, LogicalGet &get);

	//! Inject cache filter expression on a LogicalFilter using ROW_ID from the LogicalGet
	static void InjectCacheExpression(LogicalFilter &filter, LogicalGet &get,
	                                  const shared_ptr<ConditionCacheEntry> &entry);
};

} // namespace duckdb

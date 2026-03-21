#pragma once

#include "query_condition_cache_state.hpp"

#include "duckdb/main/client_context_state.hpp"
#include "duckdb/optimizer/optimizer_extension.hpp"

namespace duckdb {

//! Query-scoped state for passing cache entries between pre-optimize and post-optimize phases.
//! Stored in ClientContext::registered_state; automatically cleared on QueryEnd.
struct CacheOptimizerQueryState : public ClientContextState {
	unordered_map<idx_t, shared_ptr<ConditionCacheEntry>> cache_apply_pending;

	void QueryEnd(ClientContext &context, optional_ptr<ErrorData> error) override {
		cache_apply_pending.clear();
	}
};

class QueryConditionCacheOptimizer : public OptimizerExtension {
public:
	QueryConditionCacheOptimizer();

	//! Pre-optimize: compute canonical predicate keys before FilterPushdown splits the WHERE clause.
	//! On cache miss, builds cache inline so the first query benefits immediately.
	static void PreOptimizeFunction(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan);

	//! Post-optimize: inject cache filters into LogicalGet nodes that were matched pre-optimize
	static void OptimizeFunction(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan);

private:
	//! Check if use_query_condition_cache setting is enabled
	static bool IsSettingEnabled(ClientContext &context);

	//! Walk plan pre-pushdown: find LogicalFilter→LogicalGet, compute key, lookup/build cache
	static void PreOptimizeWalk(ClientContext &context, unique_ptr<LogicalOperator> &plan, bool inside_dml,
	                            CacheOptimizerQueryState &state);

	//! Walk plan post-pushdown: inject cache filters for pending entries
	static void PostOptimizeWalk(unique_ptr<LogicalOperator> &plan, CacheOptimizerQueryState &state);

	//! Compute a canonical predicate key from filter expressions (sorted, joined with ";")
	static CacheKey ComputePredicateKey(idx_t table_oid, const vector<unique_ptr<Expression>> &expressions);

	//! Build cache entry for a predicate on a table, store in cache store, and return it
	static shared_ptr<ConditionCacheEntry>
	BuildCacheForPredicate(ClientContext &context, const vector<unique_ptr<Expression>> &expressions, LogicalGet &get);

	//! Inject cache filter on a LogicalGet via ROW_ID table filter
	static void InjectCacheFilter(unique_ptr<LogicalOperator> &get_plan, const shared_ptr<ConditionCacheEntry> &entry);
};

} // namespace duckdb

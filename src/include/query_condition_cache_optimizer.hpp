#pragma once

#include "query_condition_cache_state.hpp"

#include "duckdb/main/client_context_state.hpp"
#include "duckdb/optimizer/optimizer_extension.hpp"

namespace duckdb {

class DuckTableEntry;
class LogicalGet;

// Query-scoped state for passing cache entries between pre-optimize and post-optimize phases.
// Stored in ClientContext::registered_state; automatically cleared on QueryEnd.
struct CacheOptimizerQueryState : public ClientContextState {
	static constexpr const char *NAME = "qcc_optimizer_state";

	// Maps table_index -> cache entry for tables matched during pre-optimize.
	// Consumed by post-optimize to inject cache filters.
	unordered_map<idx_t, shared_ptr<ConditionCacheEntry>> cache_apply_pending;

	void QueryEnd(ClientContext &context, optional_ptr<ErrorData> error) override {
		cache_apply_pending.clear();
	}
};

class QueryConditionCacheOptimizer : public OptimizerExtension {
public:
	QueryConditionCacheOptimizer();

	// Pre-optimize: compute canonical predicate keys before FilterPushdown splits the WHERE clause.
	// On cache miss, build the cache inline so the first query can reuse it immediately.
	static void PreOptimizeFunction(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan);

	// Post-optimize: inject cache filters into LogicalGet nodes matched during pre-optimize.
	static void OptimizeFunction(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan);

private:
	static bool IsSettingEnabled(ClientContext &context);

	static void PreOptimizeWalk(ClientContext &context, unique_ptr<LogicalOperator> &plan, bool inside_dml,
	                            CacheOptimizerQueryState &state);
	static void PostOptimizeWalk(unique_ptr<LogicalOperator> &plan, CacheOptimizerQueryState &state);

	static CacheKey ComputePredicateKey(ClientContext &context, idx_t table_oid,
	                                    const vector<unique_ptr<Expression>> &expressions, LogicalGet &get);
	static shared_ptr<ConditionCacheEntry>
	BuildCacheForPredicate(ClientContext &context, const vector<unique_ptr<Expression>> &expressions, LogicalGet &get);

	static string ReconstructPredicateSQL(const vector<unique_ptr<Expression>> &expressions);
	static void InjectCacheFilter(unique_ptr<LogicalOperator> &get_plan, const shared_ptr<ConditionCacheEntry> &entry);
};

} // namespace duckdb

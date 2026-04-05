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
	// On cache miss, builds cache inline so the first query benefits immediately.
	static void PreOptimizeFunction(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan);

	// Post-optimize: inject cache filters into LogicalGet nodes that were matched pre-optimize.
	static void OptimizeFunction(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan);

private:
	static bool IsSettingEnabled(ClientContext &context);

	// Walk plan before FilterPushdown: find LogicalFilter -> LogicalGet, compute key, lookup/build cache
	static void PreOptimizeWalk(ClientContext &context, unique_ptr<LogicalOperator> &plan, bool inside_dml,
	                            CacheOptimizerQueryState &state);

	// Compute a canonical predicate key via parse+bind normalization,
	// matching the key format used by condition_cache_build/info.
	static CacheKey ComputePredicateKey(ClientContext &context, idx_t table_oid,
	                                    const vector<unique_ptr<Expression>> &expressions, LogicalGet &get);

	// Build cache entry for a predicate on a table
	static shared_ptr<ConditionCacheEntry>
	BuildCacheForPredicate(ClientContext &context, const vector<unique_ptr<Expression>> &expressions, LogicalGet &get);

	// Walk plan after built-in optimization and inject cache filters into matching table scans.
	static void PostOptimizeWalk(ClientContext &context, unique_ptr<LogicalOperator> &plan,
	                             CacheOptimizerQueryState &state);

	// Inject a rowid-backed cache filter into a LogicalGet while preserving its visible output.
	static void InjectCacheFilter(ClientContext &context, LogicalGet &get,
	                              const shared_ptr<ConditionCacheEntry> &entry);

	// Reconstruct SQL from filter expressions, sort for alphabetical ordering
	static string ReconstructPredicateSQL(const vector<unique_ptr<Expression>> &expressions);
};

} // namespace duckdb

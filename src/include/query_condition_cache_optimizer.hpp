#pragma once

#include "query_condition_cache_state.hpp"

#include "duckdb/main/client_context_state.hpp"
#include "duckdb/optimizer/optimizer_extension.hpp"

namespace duckdb {

class DuckTableEntry;
class LogicalGet;

struct CacheOptimizerCandidate {
	CacheKey key;
	shared_ptr<ConditionCacheEntry> entry;
	bool cache_hit;
	bool allow_finalize_backfill;
};

// Query-scoped state for passing cache entries between pre-optimize and post-optimize phases.
// Stored in ClientContext::registered_state; automatically cleared on QueryEnd.
struct CacheOptimizerQueryState : public ClientContextState {
	static constexpr const char *NAME = "qcc_optimizer_state";

	// Maps table_index -> cache candidate discovered before built-in filter pushdown.
	// Consumed after built-in optimization to apply existing entries or install
	// a fused cache-building filter for supported cold misses.
	unordered_map<idx_t, CacheOptimizerCandidate> cache_candidates;

	void QueryEnd(ClientContext &context, optional_ptr<ErrorData> error) override {
		cache_candidates.clear();
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
	                            bool inside_truncating, CacheOptimizerQueryState &state);

	// Build cache entry for a predicate on a table
	static shared_ptr<ConditionCacheEntry>
	BuildCacheForPredicate(ClientContext &context, const vector<unique_ptr<Expression>> &expressions, LogicalGet &get);

	// Walk plan after built-in optimization and inject cache filters into matching table scans.
	static void PostOptimizeWalk(ClientContext &context, unique_ptr<LogicalOperator> &plan,
	                             CacheOptimizerQueryState &state);

	// Inject a rowid-backed cache filter into a LogicalGet while preserving its visible output.
	static void InjectCacheFilter(ClientContext &context, LogicalGet &get,
	                              const shared_ptr<ConditionCacheEntry> &entry);

	static bool TryInstallFusedRecorder(ClientContext &context, unique_ptr<LogicalOperator> &plan,
	                                    CacheOptimizerCandidate &candidate);
};

} // namespace duckdb

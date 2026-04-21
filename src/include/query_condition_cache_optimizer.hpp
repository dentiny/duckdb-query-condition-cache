#pragma once

#include "query_condition_cache_state.hpp"

#include "duckdb/main/client_context_state.hpp"
#include "duckdb/optimizer/optimizer_extension.hpp"

namespace duckdb {

class DuckTableEntry;
class LogicalGet;

struct RecorderInjectionInfo {
	idx_t table_oid;
	string canonical_key;
	// BoundColumnRefs already remapped to chunk positions for the recorder.
	unique_ptr<Expression> predicate;
};

// Query-scoped state passed from pre-optimize to post-optimize.
struct CacheOptimizerQueryState : public ClientContextState {
	static constexpr const char *NAME = "qcc_optimizer_state";

	// table_index -> entry to bind the filter against.
	unordered_map<idx_t, shared_ptr<ConditionCacheEntry>> cache_apply_pending;
	// table_index -> recorder injection info, populated on cache miss only.
	unordered_map<idx_t, RecorderInjectionInfo> cache_recorder_pending;

	void QueryEnd(ClientContext &context, optional_ptr<ErrorData> error) override {
		cache_apply_pending.clear();
		cache_recorder_pending.clear();
	}
};

class QueryConditionCacheOptimizer : public OptimizerExtension {
public:
	QueryConditionCacheOptimizer();

	static void PreOptimizeFunction(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan);
	static void OptimizeFunction(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan);

private:
	static bool IsSettingEnabled(ClientContext &context);

	static void PreOptimizeWalk(ClientContext &context, unique_ptr<LogicalOperator> &plan, bool inside_dml,
	                            CacheOptimizerQueryState &state);

	static void PostOptimizeWalk(ClientContext &context, unique_ptr<LogicalOperator> &plan,
	                             CacheOptimizerQueryState &state);

	static void InjectCacheFilter(ClientContext &context, LogicalGet &get,
	                              const shared_ptr<ConditionCacheEntry> &entry);

	// Wraps `plan` (a LogicalGet) with a LogicalCacheRecorder; reassigns `plan` so the
	// parent's children pointer updates transparently.
	static void InjectCacheRecorder(ClientContext &context, unique_ptr<LogicalOperator> &plan,
	                                RecorderInjectionInfo &&info);
};

} // namespace duckdb

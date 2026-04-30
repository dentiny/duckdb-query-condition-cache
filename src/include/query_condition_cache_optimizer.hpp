#pragma once

#include "query_condition_cache_state.hpp"

#include "duckdb/main/client_context_state.hpp"
#include "duckdb/optimizer/optimizer_extension.hpp"
#include "duckdb/planner/expression.hpp"

namespace duckdb {

class LogicalGet;

struct RecorderInjectionInfo {
	idx_t table_oid;
	string table_catalog;
	string table_schema;
	string table_name;
	string canonical_key;
	unique_ptr<Expression> predicate;
	unique_ptr<Expression> backfill_predicate;
	shared_ptr<ConditionCacheEntry> entry;
	shared_ptr<ConditionCacheEntry> metadata_entry;
};

struct CacheOptimizerQueryState : public ClientContextState {
	static constexpr const char *NAME = "qcc_optimizer_state";

	unordered_map<idx_t, shared_ptr<ConditionCacheEntry>> cache_apply_pending;
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
	                            bool inside_truncating, CacheOptimizerQueryState &state);
	static void PostOptimizeWalk(ClientContext &context, unique_ptr<LogicalOperator> &plan,
	                             CacheOptimizerQueryState &state);

	static void InjectCacheFilter(ClientContext &context, LogicalGet &get,
	                              const shared_ptr<ConditionCacheEntry> &entry);
	static void InjectCacheRecorder(unique_ptr<LogicalOperator> &plan, idx_t table_oid, string canonical_key,
	                                string table_catalog, string table_schema, string table_name,
	                                unique_ptr<Expression> predicate, unique_ptr<Expression> backfill_predicate,
	                                const shared_ptr<ConditionCacheEntry> &entry,
	                                const shared_ptr<ConditionCacheEntry> &metadata_entry);
	static shared_ptr<ConditionCacheEntry> GetPrunedRowGroupsFromTableFilters(ClientContext &context,
	                                                                          const LogicalGet &get);
	static idx_t EnsureRowIdChunkIndex(LogicalGet &get);
};

} // namespace duckdb

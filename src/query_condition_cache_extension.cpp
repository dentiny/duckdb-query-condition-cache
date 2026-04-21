#define DUCKDB_EXTENSION_MAIN

#include "query_condition_cache_extension.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/optimizer/optimizer_extension.hpp"
#include "cache_invalidation_optimizer.hpp"
#include "logical_cache_invalidator.hpp"
#include "query_condition_cache_filter.hpp"
#include "query_condition_cache_functions.hpp"
#include "query_condition_cache_optimizer.hpp"
#include "query_condition_cache_state.hpp"

namespace duckdb {

namespace {

// Clear all cache entries when the setting is disabled
void OnQueryConditionCacheSettingChange(ClientContext &context, SetScope scope, Value &parameter) {
	if (!parameter.GetValue<bool>()) {
		auto store = ConditionCacheStore::GetOrCreate(context);
		store->ClearAll(context);
	}
}

void LoadInternal(ExtensionLoader &loader) {
	loader.RegisterFunction(ConditionCacheBuildFunction());
	loader.RegisterFunction(ConditionCacheInfoFunction());
	loader.RegisterFunction(ConditionCacheStatsFunction());
	loader.RegisterFunction(ConditionCacheResetStatsFunction());

	// Register the internal filter function so it survives plan serialization/verification
	loader.RegisterFunction(ConditionCacheFilterFunction());

	// Register the use_query_condition_cache setting (default: true)
	auto &db = loader.GetDatabaseInstance();
	auto &config = DBConfig::GetConfig(db);
	config.AddExtensionOption("use_query_condition_cache", "Enable automatic query condition cache build and apply",
	                          LogicalType {LogicalTypeId::BOOLEAN}, Value::BOOLEAN(true),
	                          OnQueryConditionCacheSettingChange);

	// Register optimizer extension
	OptimizerExtension::Register(config, QueryConditionCacheOptimizer());
	OptimizerExtension::Register(config, CacheInvalidationOptimizer());
	OperatorExtension::Register(config, make_shared_ptr<CacheInvalidatorOperatorExtension>());
}
} // namespace

void QueryConditionCacheExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}

std::string QueryConditionCacheExtension::Name() {
	return "query_condition_cache";
}

std::string QueryConditionCacheExtension::Version() const {
#ifdef EXT_VERSION_QUERY_CONDITION_CACHE
	return EXT_VERSION_QUERY_CONDITION_CACHE;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {
DUCKDB_CPP_EXTENSION_ENTRY(query_condition_cache, loader) {
	duckdb::LoadInternal(loader);
}
}

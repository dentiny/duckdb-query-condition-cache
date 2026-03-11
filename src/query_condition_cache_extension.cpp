#define DUCKDB_EXTENSION_MAIN

#include "query_condition_cache_extension.hpp"

#include "duckdb/main/config.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "query_condition_cache_functions.hpp"
#include "query_condition_cache_optimizer.hpp"

namespace duckdb {

namespace {
void LoadInternal(ExtensionLoader &loader) {
	loader.RegisterFunction(ConditionCacheBuildFunction());
	loader.RegisterFunction(ConditionCacheInfoFunction());

	// Register the use_query_condition_cache setting (default: false)
	auto &db = loader.GetDatabaseInstance();
	auto &config = DBConfig::GetConfig(db);
	config.AddExtensionOption("use_query_condition_cache", "Enable automatic query condition cache build and apply",
	                          LogicalType {LogicalTypeId::BOOLEAN}, Value::BOOLEAN(false));

	// Register optimizer extensions
	config.optimizer_extensions.push_back(QueryConditionCacheOptimizer());
	config.optimizer_extensions.push_back(CacheInvalidationOptimizer());
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

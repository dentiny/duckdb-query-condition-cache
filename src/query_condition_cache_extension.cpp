#define DUCKDB_EXTENSION_MAIN

#include "query_condition_cache_extension.hpp"

#include "duckdb/main/config.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "logical_cache_invalidator.hpp"
#include "query_condition_cache_filter.hpp"
#include "duckdb/optimizer/optimizer_extension.hpp"
#include "query_condition_cache_functions.hpp"
#include "query_condition_cache_optimizer.hpp"

namespace duckdb {

namespace {

void LoadInternal(ExtensionLoader &loader) {
	loader.RegisterFunction(ConditionCacheBuildFunction());
	loader.RegisterFunction(ConditionCacheInfoFunction());

	// Register the internal filter function so it survives plan serialization/verification
	ScalarFunction cache_filter_func("__condition_cache_filter", {LogicalType {LogicalTypeId::BIGINT}},
	                                 LogicalType {LogicalTypeId::BOOLEAN}, ConditionCacheFilterFn,
	                                 ConditionCacheFilterBind, nullptr, nullptr, ConditionCacheFilterInit);
	loader.RegisterFunction(cache_filter_func);

	// Register the use_query_condition_cache setting (default: false)
	auto &db = loader.GetDatabaseInstance();
	auto &config = DBConfig::GetConfig(db);
	config.AddExtensionOption("use_query_condition_cache", "Enable automatic query condition cache build and apply",
	                          LogicalType {LogicalTypeId::BOOLEAN}, Value::BOOLEAN(false));

	// Register operator extension for serialization/deserialization support
	OperatorExtension::Register(config, make_uniq<CacheInvalidatorOperatorExtension>());

	// Register optimizer extensions
	OptimizerExtension::Register(config, QueryConditionCacheOptimizer());
	OptimizerExtension::Register(config, CacheInvalidationOptimizer());
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

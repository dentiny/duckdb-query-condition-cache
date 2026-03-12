#define DUCKDB_EXTENSION_MAIN

#include "query_condition_cache_extension.hpp"

#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/common/exception.hpp"
#include "query_condition_cache_functions.hpp"

namespace duckdb {

namespace {
// Callback to verify the setting is being accessed
// This callback throws an exception to confirm it's being called
void EnableQueryConditionCacheCallback(ClientContext &context, SetScope scope, Value &parameter) {
	// Throw an error to verify this callback is being called
	throw InvalidInputException("enable_query_condition_cache callback was called! Setting value: %s",
	                            parameter.ToString());
}

void LoadInternal(ExtensionLoader &loader) {
	loader.RegisterFunction(ConditionCacheBuildFunction());

	// Register extension settings
	auto &db_instance = loader.GetDatabaseInstance();
	auto &config = DBConfig::GetConfig(db_instance);
	config.AddExtensionOption("enable_query_condition_cache", "Enable or disable the query condition cache feature",
	                          LogicalType {LogicalTypeId::BOOLEAN}, Value(true), EnableQueryConditionCacheCallback);
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

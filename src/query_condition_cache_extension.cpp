#define DUCKDB_EXTENSION_MAIN

#include "query_condition_cache_extension.hpp"

#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/common/exception.hpp"
#include "query_condition_cache_functions.hpp"
#include "query_condition_cache_state.hpp"

namespace duckdb {

namespace {
void EnableQueryConditionCacheCallback(ClientContext &context, SetScope scope, Value &parameter) {
	bool enabled = parameter.GetValue<bool>();
	if (!enabled) {
		auto store = ConditionCacheStore::GetOrCreate(context);
		store->ClearAll(context);
	}
}

void LoadInternal(ExtensionLoader &loader) {
	loader.RegisterFunction(ConditionCacheBuildFunction());
	loader.RegisterFunction(ConditionCacheInfoFunction());

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

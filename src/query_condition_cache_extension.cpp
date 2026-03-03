#define DUCKDB_EXTENSION_MAIN

#include "query_condition_cache_extension.hpp"

#include "duckdb/function/table_function.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

namespace duckdb {

// Defined in query_condition_cache_functions.cpp
TableFunction ConditionCacheBuildFunction();

namespace {
void LoadInternal(ExtensionLoader &loader) {
	loader.RegisterFunction(ConditionCacheBuildFunction());
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

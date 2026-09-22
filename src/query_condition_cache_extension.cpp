#define DUCKDB_EXTENSION_MAIN

#include "query_condition_cache_extension.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/optimizer/optimizer_extension.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "cache_invalidation_optimizer.hpp"
#include "logical_cache_invalidator.hpp"
#include "query_condition_cache_filter.hpp"
#include "query_condition_cache_functions.hpp"
#include "query_condition_cache_optimizer.hpp"
#include "query_condition_cache_state.hpp"

namespace duckdb {

namespace {

FunctionDescription CreateDescription(vector<string> parameter_names, string description, vector<string> examples,
                                      vector<string> categories) {
	FunctionDescription result;
	result.parameter_names = std::move(parameter_names);
	result.description = std::move(description);
	result.examples = std::move(examples);
	result.categories = std::move(categories);
	return result;
}

void RegisterTableFunction(ExtensionLoader &loader, TableFunction function, vector<string> parameter_names,
                           string description, vector<string> examples, vector<string> categories) {
	CreateTableFunctionInfo info(std::move(function));
	info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
	info.descriptions.push_back(CreateDescription(std::move(parameter_names), std::move(description),
	                                              std::move(examples), std::move(categories)));
	loader.RegisterFunction(std::move(info));
}

void RegisterScalarFunction(ExtensionLoader &loader, ScalarFunction function, vector<string> parameter_names,
                            string description, vector<string> examples, vector<string> categories) {
	CreateScalarFunctionInfo info(std::move(function));
	info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
	info.descriptions.push_back(CreateDescription(std::move(parameter_names), std::move(description),
	                                              std::move(examples), std::move(categories)));
	loader.RegisterFunction(std::move(info));
}

// Clear all cache entries when the setting is disabled
void OnQueryConditionCacheSettingChange(ClientContext &context, SetScope scope, Value &parameter) {
	if (!parameter.GetValue<bool>()) {
		auto store = ConditionCacheStore::GetOrCreate(context);
		store->ClearAll(context);
	}
}

void LoadInternal(ExtensionLoader &loader) {
	RegisterTableFunction(
	    loader, ConditionCacheBuildFunction(),
	    /*parameter_names=*/ {"table_name", "predicate"},
	    /*description=*/"Builds or replaces a query condition cache for rows in a table that match a SQL predicate.",
	    /*examples=*/ {"SELECT * FROM condition_cache_build('events', 'event_type = ''error''');"},
	    /*categories=*/ {"cache", "query_condition_cache"});
	RegisterTableFunction(
	    loader, ConditionCacheInfoFunction(),
	    /*parameter_names=*/ {"table_name", "predicate"},
	    /*description=*/
	    "Returns row-group and vector coverage for a cached table and SQL predicate, or zeroes when no entry exists.",
	    /*examples=*/ {"SELECT * FROM condition_cache_info('events', 'event_type = ''error''');"},
	    /*categories=*/ {"cache", "query_condition_cache"});
	RegisterTableFunction(loader, ConditionCacheStatsFunction(),
	                      /*parameter_names=*/ {},
	                      /*description=*/"Returns query condition cache memory use, hit count, and access count.",
	                      /*examples=*/ {"SELECT * FROM condition_cache_stats();"},
	                      /*categories=*/ {"cache", "query_condition_cache"});
	RegisterScalarFunction(loader, ConditionCacheResetStatsFunction(),
	                       /*parameter_names=*/ {},
	                       /*description=*/"Resets query condition cache hit and access counters and returns true.",
	                       /*examples=*/ {"SELECT condition_cache_reset_stats();"},
	                       /*categories=*/ {"cache", "query_condition_cache"});

	// Register the internal filter function so it survives plan serialization/verification
	RegisterScalarFunction(
	    loader, ConditionCacheFilterFunction(),
	    /*parameter_names=*/ {"row_id"},
	    /*description=*/"Internal optimizer filter that checks whether the cache permits scanning a row ID's vector.",
	    /*examples=*/ {"SELECT __condition_cache_filter(0);"},
	    /*categories=*/ {"cache", "query_condition_cache"});

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

#include "catch/catch.hpp"
#include "query_condition_cache_filter.hpp"
#include "query_condition_cache_state.hpp"

#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/profiling_node.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/filter/expression_filter.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/storage/statistics/numeric_stats.hpp"
#include "test_helpers.hpp"

namespace duckdb {

namespace {

optional_ptr<LogicalGet> FindLogicalGet(LogicalOperator &op) {
	if (op.type == LogicalOperatorType::LOGICAL_GET) {
		return op.Cast<LogicalGet>();
	}
	for (auto &child : op.children) {
		auto result = FindLogicalGet(*child);
		if (result) {
			return result;
		}
	}
	return nullptr;
}

optional_ptr<ProfilingNode> FindConditionCacheProfilingNode(ProfilingNode &node) {
	auto &info = node.GetProfilingInfo();
	if (info.metrics.find(MetricType::EXTRA_INFO) != info.metrics.end()) {
		auto extra_info = info.GetMetricValue<InsertionOrderPreservingMap<string>>(MetricType::EXTRA_INFO);
		if (extra_info.find("Condition Cache") != extra_info.end()) {
			return node;
		}
	}
	for (idx_t i = 0; i < node.GetChildCount(); ++i) {
		auto child = FindConditionCacheProfilingNode(*node.GetChild(i));
		if (child) {
			return child;
		}
	}
	return nullptr;
}

} // namespace

TEST_CASE("CacheExpressionFilter - CheckStatistics", "[query_condition_cache]") {
	auto entry = make_shared_ptr<ConditionCacheEntry>();
	entry->SetQualifyingVector(/*rg_idx=*/0, /*vec_idx=*/0);
	entry->SetQualifyingVector(/*rg_idx=*/0, /*vec_idx=*/5);
	entry->EnsureRowGroup(/*rg_idx=*/1);
	entry->SetQualifyingVector(/*rg_idx=*/2, /*vec_idx=*/10);

	auto dummy_expr = make_uniq<BoundReferenceExpression>(LogicalType {LogicalTypeId::BIGINT}, 0);
	CacheExpressionFilter filter(std::move(dummy_expr), entry);

	SECTION("row group with qualifying vectors: no pruning") {
		auto stats = NumericStats::CreateUnknown(LogicalType {LogicalTypeId::BIGINT});
		NumericStats::SetMin(stats, Value::BIGINT(0));
		NumericStats::SetMax(stats, Value::BIGINT(100));
		REQUIRE(filter.CheckStatistics(stats) == FilterPropagateResult::NO_PRUNING_POSSIBLE);
	}

	SECTION("known-empty row group: prune") {
		auto stats = NumericStats::CreateUnknown(LogicalType {LogicalTypeId::BIGINT});
		NumericStats::SetMin(stats, Value::BIGINT(122880));
		NumericStats::SetMax(stats, Value::BIGINT(200000));
		REQUIRE(filter.CheckStatistics(stats) == FilterPropagateResult::FILTER_ALWAYS_FALSE);
	}

	SECTION("row group 2 with qualifying vectors: no pruning") {
		auto stats = NumericStats::CreateUnknown(LogicalType {LogicalTypeId::BIGINT});
		NumericStats::SetMin(stats, Value::BIGINT(245760));
		NumericStats::SetMax(stats, Value::BIGINT(300000));
		REQUIRE(filter.CheckStatistics(stats) == FilterPropagateResult::NO_PRUNING_POSSIBLE);
	}

	SECTION("stats without min/max: no pruning") {
		auto stats = NumericStats::CreateUnknown(LogicalType {LogicalTypeId::BIGINT});
		REQUIRE(filter.CheckStatistics(stats) == FilterPropagateResult::NO_PRUNING_POSSIBLE);
	}
}

TEST_CASE("Optimizer injects cache filter into LogicalGet", "[query_condition_cache]") {
	DuckDB db(nullptr);
	Connection con(db);

	REQUIRE_NO_FAIL(con.Query("LOAD query_condition_cache"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE t AS SELECT i AS id, i % 100 AS val FROM range(1000) t(i)"));
	REQUIRE_NO_FAIL(con.Query("SELECT status FROM condition_cache_build('t', 'val = 42')"));

	auto require_rowid_cache_filter = [&](const string &query) {
		auto plan = con.ExtractPlan(query);
		REQUIRE(plan != nullptr);

		auto get = FindLogicalGet(*plan);
		REQUIRE(get);

		auto rowid_filter = get->table_filters.filters.find(COLUMN_IDENTIFIER_ROW_ID);
		REQUIRE(rowid_filter != get->table_filters.filters.end());

		auto &expr_filter = rowid_filter->second->Cast<ExpressionFilter>();
		REQUIRE(expr_filter.expr->GetExpressionType() == ExpressionType::BOUND_FUNCTION);

		auto &function_expr = expr_filter.expr->Cast<BoundFunctionExpression>();
		REQUIRE(function_expr.function.name == "__condition_cache_filter");
		REQUIRE(function_expr.bind_info != nullptr);

		auto &bind_data = function_expr.bind_info->Cast<ConditionCacheFilterBindData>();
		REQUIRE(bind_data.cache_entry != nullptr);
		REQUIRE(bind_data.profile_info != nullptr);
		REQUIRE(get->function.dynamic_to_string == ConditionCacheDynamicToString);

		auto &scan_bind_data = get->bind_data->Cast<ConditionCacheTableScanBindData>();
		REQUIRE(scan_bind_data.profile_info != nullptr);
		return plan;
	};

	SECTION("count(*) query appends hidden rowid column") {
		auto plan = require_rowid_cache_filter("SELECT count(*) FROM t WHERE val = 42");
		auto get = FindLogicalGet(*plan);
		REQUIRE(get);
		REQUIRE(get->table_filters.filters.count(COLUMN_IDENTIFIER_ROW_ID) == 1);
	}

	SECTION("query selecting rowid still receives cache filter") {
		auto plan = require_rowid_cache_filter("SELECT rowid FROM t WHERE val = 42");
		auto get = FindLogicalGet(*plan);
		REQUIRE(get);
		REQUIRE(get->table_filters.filters.count(COLUMN_IDENTIFIER_ROW_ID) == 1);
	}

	SECTION("regular projection query still receives cache filter") {
		auto plan = require_rowid_cache_filter("SELECT id FROM t WHERE val = 42");
		auto get = FindLogicalGet(*plan);
		REQUIRE(get);
		REQUIRE(get->table_filters.filters.count(COLUMN_IDENTIFIER_ROW_ID) == 1);
	}

	SECTION("setting disabled skips injection") {
		REQUIRE_NO_FAIL(con.Query("SET use_query_condition_cache = false"));

		auto plan = con.ExtractPlan("SELECT count(*) FROM t WHERE val = 42");
		REQUIRE(plan != nullptr);

		auto get = FindLogicalGet(*plan);
		REQUIRE(get);
		REQUIRE(get->table_filters.filters.find(COLUMN_IDENTIFIER_ROW_ID) == get->table_filters.filters.end());
	}
}

TEST_CASE("Condition cache profiling extra info is surfaced in explain analyze and JSON profiling",
          "[query_condition_cache][profile]") {
	DuckDB db(nullptr);
	Connection con(db);

	REQUIRE_NO_FAIL(con.Query("LOAD query_condition_cache"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE t AS SELECT i AS id FROM range(500000) t(i)"));

	auto explain_result = con.Query("EXPLAIN ANALYZE SELECT count(*) FROM t WHERE id < 3000");
	REQUIRE(explain_result);
	REQUIRE(!explain_result->HasError());

	auto explain_text = explain_result->GetValue(1, 0).ToString();
	REQUIRE(explain_text.find("Condition Cache") != string::npos);
	REQUIRE(explain_text.find("MISS -> BUILT") != string::npos);

	con.EnableProfiling();
	con.context->config.emit_profiler_output = false;

	auto query_result = con.Query("SELECT count(*) FROM t WHERE id < 3000");
	REQUIRE(query_result);
	REQUIRE(!query_result->HasError());
	REQUIRE(query_result->GetValue(0, 0).GetValue<int64_t>() == 3000);

	auto profiling_root = con.GetProfilingTree();
	REQUIRE(profiling_root);

	auto condition_cache_node = FindConditionCacheProfilingNode(*profiling_root);
	REQUIRE(condition_cache_node);

	auto extra_info = condition_cache_node->GetProfilingInfo().GetMetricValue<InsertionOrderPreservingMap<string>>(
	    MetricType::EXTRA_INFO);

	auto status = extra_info.find("Condition Cache");
	REQUIRE(status != extra_info.end());
	REQUIRE(status->second == "HIT");

	auto predicate_hash = extra_info.find("Condition Cache Predicate Hash");
	REQUIRE(predicate_hash != extra_info.end());
	REQUIRE(!predicate_hash->second.empty());

	auto cached_row_groups = extra_info.find("Condition Cache Cached Row Groups");
	REQUIRE(cached_row_groups != extra_info.end());
	REQUIRE(cached_row_groups->second == "5");

	auto qualifying_vectors = extra_info.find("Condition Cache Qualifying Vectors");
	REQUIRE(qualifying_vectors != extra_info.end());
	REQUIRE(qualifying_vectors->second == "2/245");

	auto json_profile = con.GetProfilingInformation(ProfilerPrintFormat::JSON);
	REQUIRE(json_profile.find("Condition Cache") != string::npos);
	REQUIRE(json_profile.find("HIT") != string::npos);
}
} // namespace duckdb

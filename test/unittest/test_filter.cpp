#include "catch/catch.hpp"
#include "query_condition_cache_filter.hpp"
#include "query_condition_cache_state.hpp"

#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"
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
} // namespace duckdb

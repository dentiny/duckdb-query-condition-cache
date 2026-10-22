#include "catch/catch.hpp"
#include "query_condition_cache_filter.hpp"
#include "query_condition_cache_state.hpp"

#include "duckdb/common/allocator.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/filter/expression_filter.hpp"
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

optional_idx FindRowIdProjection(const LogicalGet &get) {
	const auto &columns = get.GetColumnIds();
	for (idx_t index = 0; index < columns.size(); index++) {
		if (columns[index].IsRowIdColumn()) {
			return index;
		}
	}
	return optional_idx();
}

} // namespace

TEST_CASE("Condition cache filter - CheckStatistics", "[query_condition_cache]") {
	auto entry = make_shared_ptr<ConditionCacheEntry>();
	entry->SetQualifyingVector(/*rg_idx=*/0, /*vec_idx=*/0);
	entry->SetQualifyingVector(/*rg_idx=*/0, /*vec_idx=*/5);
	entry->EnsureRowGroup(/*rg_idx=*/1);
	entry->SetQualifyingVector(/*rg_idx=*/2, /*vec_idx=*/10);

	vector<unique_ptr<Expression>> children;
	children.push_back(make_uniq<BoundReferenceExpression>(LogicalType {LogicalTypeId::BIGINT}, 0));
	ExpressionFilter filter(make_uniq<BoundFunctionExpression>(BoundScalarFunction(ConditionCacheFilterFunction()),
	                                                           std::move(children),
	                                                           make_uniq<ConditionCacheFilterBindData>(entry)));

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

	SECTION("copied filter retains cache pruning") {
		auto copied = filter.Copy();
		auto stats = NumericStats::CreateUnknown(LogicalType {LogicalTypeId::BIGINT});
		NumericStats::SetMin(stats, Value::BIGINT(DEFAULT_ROW_GROUP_SIZE));
		NumericStats::SetMax(stats, Value::BIGINT(DEFAULT_ROW_GROUP_SIZE + 100));
		REQUIRE(copied->CheckStatistics(stats) == FilterPropagateResult::FILTER_ALWAYS_FALSE);
	}

	SECTION("negative row IDs: no pruning") {
		auto stats = NumericStats::CreateUnknown(LogicalType {LogicalTypeId::BIGINT});
		NumericStats::SetMin(stats, Value::BIGINT(-1));
		NumericStats::SetMax(stats, Value::BIGINT(100));
		REQUIRE(filter.CheckStatistics(stats) == FilterPropagateResult::NO_PRUNING_POSSIBLE);
	}

	SECTION("stats without min/max: no pruning") {
		auto stats = NumericStats::CreateUnknown(LogicalType {LogicalTypeId::BIGINT});
		REQUIRE(filter.CheckStatistics(stats) == FilterPropagateResult::NO_PRUNING_POSSIBLE);
	}
}

TEST_CASE("ConditionCacheFilterFn handles scan vectors crossing cache-vector boundaries", "[query_condition_cache]") {
	DuckDB db(nullptr);
	Connection con(db);

	const auto entry = make_shared_ptr<ConditionCacheEntry>();
	entry->EnsureRowGroup(/*rg_idx=*/0);
	entry->SetQualifyingVector(/*rg_idx=*/0, /*vec_idx=*/1);

	vector<unique_ptr<Expression>> children;
	children.push_back(make_uniq<BoundReferenceExpression>(LogicalType {LogicalTypeId::BIGINT}, 0));
	const auto expression =
	    make_uniq<BoundFunctionExpression>(BoundScalarFunction(ConditionCacheFilterFunction()), std::move(children),
	                                       make_uniq<ConditionCacheFilterBindData>(entry));
	ExpressionExecutor executor(*con.context, *expression);

	DataChunk input;
	input.Initialize(Allocator::Get(*con.context), {LogicalType {LogicalTypeId::BIGINT}});
	input.SetCardinality(STANDARD_VECTOR_SIZE);
	const auto row_ids = FlatVector::GetData<int64_t>(input.data[0]);
	for (idx_t idx = 0; idx < STANDARD_VECTOR_SIZE; idx++) {
		row_ids[idx] = NumericCast<int64_t>(1536 + idx);
	}

	Vector result(LogicalType {LogicalTypeId::BOOLEAN});
	executor.ExecuteExpression(input, result);

	for (idx_t idx = 0; idx < 512; idx++) {
		REQUIRE(!result.GetValue(idx).GetValue<bool>());
	}
	for (idx_t idx = 512; idx < STANDARD_VECTOR_SIZE; idx++) {
		REQUIRE(result.GetValue(idx).GetValue<bool>());
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

		auto rowid_index = FindRowIdProjection(*get);
		REQUIRE(rowid_index.IsValid());
		auto rowid_filter = get->table_filters.TryGetFilterByColumnIndex(ProjectionIndex(rowid_index.GetIndex()));
		REQUIRE(rowid_filter);

		auto &expr_filter = rowid_filter->Cast<ExpressionFilter>();
		REQUIRE(expr_filter.expr->GetExpressionType() == ExpressionType::BOUND_FUNCTION);

		auto &function_expr = expr_filter.expr->Cast<BoundFunctionExpression>();
		REQUIRE(function_expr.Function().GetName() == "__condition_cache_filter");
		REQUIRE(function_expr.BindInfo() != nullptr);

		auto &bind_data = function_expr.BindInfo()->Cast<ConditionCacheFilterBindData>();
		REQUIRE(bind_data.cache_entry != nullptr);
		return plan;
	};

	SECTION("count(*) query appends hidden rowid column") {
		auto plan = require_rowid_cache_filter("SELECT count(*) FROM t WHERE val = 42");
		auto get = FindLogicalGet(*plan);
		REQUIRE(get);
		REQUIRE(get->table_filters.HasFilter(ProjectionIndex(FindRowIdProjection(*get).GetIndex())));
	}

	SECTION("query selecting rowid still receives cache filter") {
		auto plan = require_rowid_cache_filter("SELECT rowid FROM t WHERE val = 42");
		auto get = FindLogicalGet(*plan);
		REQUIRE(get);
		REQUIRE(get->table_filters.HasFilter(ProjectionIndex(FindRowIdProjection(*get).GetIndex())));
	}

	SECTION("regular projection query still receives cache filter") {
		auto plan = require_rowid_cache_filter("SELECT id FROM t WHERE val = 42");
		auto get = FindLogicalGet(*plan);
		REQUIRE(get);
		REQUIRE(get->table_filters.HasFilter(ProjectionIndex(FindRowIdProjection(*get).GetIndex())));
	}

	SECTION("setting disabled skips injection") {
		REQUIRE_NO_FAIL(con.Query("SET use_query_condition_cache = false"));

		auto plan = con.ExtractPlan("SELECT count(*) FROM t WHERE val = 42");
		REQUIRE(plan != nullptr);

		auto get = FindLogicalGet(*plan);
		REQUIRE(get);
		auto rowid_index = FindRowIdProjection(*get);
		if (rowid_index.IsValid()) {
			REQUIRE_FALSE(get->table_filters.HasFilter(ProjectionIndex(rowid_index.GetIndex())));
		}
	}
}
} // namespace duckdb

#include "catch/catch.hpp"
#include "logical_cache_invalidator.hpp"

#include "duckdb/planner/expression/bound_reference_expression.hpp"

namespace duckdb {

TEST_CASE("LogicalCacheInvalidator - INVALIDATE mode constructor", "[logical_invalidator]") {
	auto row_id_expr = make_uniq<BoundReferenceExpression>(LogicalType {LogicalTypeId::BIGINT}, 3);
	LogicalCacheInvalidator op(42, std::move(row_id_expr));

	REQUIRE(op.table_oid == 42);
	REQUIRE(op.mode == CacheInvalidatorMode::INVALIDATE);
	REQUIRE(op.row_id_column_index == 0);
	REQUIRE(op.pre_insert_row_count == 0);
	REQUIRE(op.expressions.size() == 1);

	auto &ref = op.expressions[0]->Cast<BoundReferenceExpression>();
	REQUIRE(ref.index == 3);
	REQUIRE(ref.return_type == LogicalType {LogicalTypeId::BIGINT});
}

TEST_CASE("LogicalCacheInvalidator - INSERT mode constructor", "[logical_invalidator]") {
	LogicalCacheInvalidator op(100, static_cast<idx_t>(50000));

	REQUIRE(op.table_oid == 100);
	REQUIRE(op.mode == CacheInvalidatorMode::INSERT);
	REQUIRE(op.row_id_column_index == 0);
	REQUIRE(op.pre_insert_row_count == 50000);
	REQUIRE(op.expressions.empty());
}

TEST_CASE("LogicalCacheInvalidator - MERGE mode constructor", "[logical_invalidator]") {
	LogicalCacheInvalidator op(200, static_cast<idx_t>(5), static_cast<idx_t>(10000));

	REQUIRE(op.table_oid == 200);
	REQUIRE(op.mode == CacheInvalidatorMode::MERGE);
	REQUIRE(op.row_id_column_index == 5);
	REQUIRE(op.pre_insert_row_count == 10000);
	REQUIRE(op.expressions.empty());
}

TEST_CASE("LogicalCacheInvalidator - GetExtensionName", "[logical_invalidator]") {
	LogicalCacheInvalidator op(1, static_cast<idx_t>(0));
	REQUIRE(op.GetExtensionName() == "query_condition_cache");
}

TEST_CASE("CacheInvalidatorOperatorExtension - GetName", "[logical_invalidator]") {
	CacheInvalidatorOperatorExtension ext;
	REQUIRE(ext.GetName() == "query_condition_cache");
}

} // namespace duckdb

#include "catch/catch.hpp"
#include "query_condition_cache_functions.hpp"
#include "query_condition_cache_state.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression_binder/check_binder.hpp"

namespace duckdb {

TEST_CASE("BuildCacheEntry - basic predicate", "[build_cache_entry]") {
	DuckDB db(nullptr);
	Connection con(db);

	con.Query("CREATE TABLE t AS SELECT i AS id, i % 100 AS val FROM range(500000) t(i)");

	auto &context = *con.context;
	con.BeginTransaction();
	auto &table_entry = Catalog::GetEntry<DuckTableEntry>(context, INVALID_CATALOG, DEFAULT_SCHEMA, "t");

	SECTION("equality predicate") {
		auto parsed_exprs = Parser::ParseExpressionList("val = 42");
		auto binder = Binder::CreateBinder(context);
		physical_index_set_t bound_columns;
		CheckBinder check_binder(*binder, context, "t", table_entry.GetColumns(), bound_columns);
		auto bound_expr = check_binder.Bind(parsed_exprs[0]);

		bound_expr =
		    BoundCastExpression::AddCastToType(context, std::move(bound_expr), LogicalType {LogicalTypeId::BOOLEAN});

		auto entry = BuildCacheEntry(context, table_entry, *bound_expr);

		REQUIRE(entry != nullptr);
		REQUIRE(entry->RowGroupCount() == 5);
		// BuildCacheEntry must finalise the watermark so VectorPassesFilter / CheckStatistics
		// trust the bitvectors for pruning.
		for (idx_t rg = 0; rg < 5; ++rg) {
			REQUIRE(entry->GetObservedVectors(rg) == VECTORS_PER_ROW_GROUP);
		}
	}

	SECTION("selective predicate") {
		auto parsed_exprs = Parser::ParseExpressionList("id < 3000");
		auto binder = Binder::CreateBinder(context);
		physical_index_set_t bound_columns;
		CheckBinder check_binder(*binder, context, "t", table_entry.GetColumns(), bound_columns);
		auto bound_expr = check_binder.Bind(parsed_exprs[0]);

		bound_expr =
		    BoundCastExpression::AddCastToType(context, std::move(bound_expr), LogicalType {LogicalTypeId::BOOLEAN});

		auto entry = BuildCacheEntry(context, table_entry, *bound_expr);

		REQUIRE(entry != nullptr);
		REQUIRE(entry->RowGroupCount() == 5);
		REQUIRE(entry->RowGroupVectorHasQualifyingRows(0, 0));
		REQUIRE(entry->RowGroupVectorHasQualifyingRows(0, 1));
		REQUIRE_FALSE(entry->RowGroupVectorHasQualifyingRows(0, 2));
		REQUIRE(entry->RowGroupIsCompletelyEmpty(1));
		for (idx_t rg = 0; rg < 5; ++rg) {
			REQUIRE(entry->GetObservedVectors(rg) == VECTORS_PER_ROW_GROUP);
		}
	}

	SECTION("odd values pass, even values don't") {
		auto parsed_exprs = Parser::ParseExpressionList("val % 2 = 1");
		auto binder = Binder::CreateBinder(context);
		physical_index_set_t bound_columns;
		CheckBinder check_binder(*binder, context, "t", table_entry.GetColumns(), bound_columns);
		auto bound_expr = check_binder.Bind(parsed_exprs[0]);

		bound_expr =
		    BoundCastExpression::AddCastToType(context, std::move(bound_expr), LogicalType {LogicalTypeId::BOOLEAN});

		auto entry = BuildCacheEntry(context, table_entry, *bound_expr);

		REQUIRE(entry != nullptr);
		REQUIRE(entry->RowGroupCount() == 5);
		for (idx_t rg = 0; rg < 5; ++rg) {
			REQUIRE(entry->GetObservedVectors(rg) == VECTORS_PER_ROW_GROUP);
		}
	}

	SECTION("no matching rows") {
		auto parsed_exprs = Parser::ParseExpressionList("id < 0");
		auto binder = Binder::CreateBinder(context);
		physical_index_set_t bound_columns;
		CheckBinder check_binder(*binder, context, "t", table_entry.GetColumns(), bound_columns);
		auto bound_expr = check_binder.Bind(parsed_exprs[0]);

		bound_expr =
		    BoundCastExpression::AddCastToType(context, std::move(bound_expr), LogicalType {LogicalTypeId::BOOLEAN});

		auto entry = BuildCacheEntry(context, table_entry, *bound_expr);

		REQUIRE(entry != nullptr);
		REQUIRE(entry->RowGroupCount() == 5);
		REQUIRE(entry->RowGroupIsCompletelyEmpty(0));
		REQUIRE(entry->RowGroupIsCompletelyEmpty(4));
		// Even when no rows match, every observed row group must be marked fully observed so
		// CheckStatistics can prune them.
		for (idx_t rg = 0; rg < 5; ++rg) {
			REQUIRE(entry->GetObservedVectors(rg) == VECTORS_PER_ROW_GROUP);
		}
	}
}
} // namespace duckdb

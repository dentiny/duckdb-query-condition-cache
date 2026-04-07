#include "catch/catch.hpp"

#include "query_condition_cache_state.hpp"

#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"

namespace duckdb {

TEST_CASE("PhysicalCacheInvalidator - INVALIDATE mode pattern", "[physical_invalidator]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto &context = *con.context;
	auto store = ConditionCacheStore::GetOrCreate(context);

	// Simulate a table with 3 row groups and a cached predicate
	auto entry = make_shared_ptr<ConditionCacheEntry>();
	entry->SetQualifyingVector(/*rg_idx=*/0, /*vec_idx=*/0);
	entry->SetQualifyingVector(/*rg_idx=*/1, /*vec_idx=*/2);
	entry->SetQualifyingVector(/*rg_idx=*/2, /*vec_idx=*/1);
	store->Upsert(context, {/*table_oid=*/1, "val > 5"}, entry);

	// ROW_ID mode: collect row group indices from affected row IDs.
	// DELETE WHERE val < 10 affects rows in row group 0 and 2.
	unordered_set<idx_t> affected_row_groups = {0, 2};
	auto removed = store->RemoveRowGroupsForTable(context, /*table_oid=*/1, affected_row_groups);
	REQUIRE(removed == 2);

	auto found = store->Lookup(context, {/*table_oid=*/1, "val > 5"});
	REQUIRE(found != nullptr);
	REQUIRE_FALSE(found->HasRowGroup(0));
	REQUIRE(found->HasRowGroup(1));
	REQUIRE_FALSE(found->HasRowGroup(2));
}

TEST_CASE("PhysicalCacheInvalidator - INSERT mode invalidation pattern", "[physical_invalidator]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto &context = *con.context;
	auto store = ConditionCacheStore::GetOrCreate(context);

	// Simulate a table with 2 row groups
	auto entry = make_shared_ptr<ConditionCacheEntry>();
	entry->SetQualifyingVector(/*rg_idx=*/0, /*vec_idx=*/0);
	entry->SetQualifyingVector(/*rg_idx=*/1, /*vec_idx=*/0);
	store->Upsert(context, {/*table_oid=*/1, "val = 5"}, entry);

	// INSERT mode: compute affected row groups from insert range.
	// Table had pre_insert_row_count rows, inserting inserted_row_count more.
	idx_t pre_insert_row_count = DEFAULT_ROW_GROUP_SIZE + 100; // somewhere in row group 1
	idx_t inserted_row_count = DEFAULT_ROW_GROUP_SIZE;         // spans into row group 2
	idx_t first_rg = pre_insert_row_count / DEFAULT_ROW_GROUP_SIZE;
	idx_t last_rg = (pre_insert_row_count + inserted_row_count - 1) / DEFAULT_ROW_GROUP_SIZE;

	unordered_set<idx_t> affected_row_groups;
	for (idx_t rg = first_rg; rg <= last_rg; ++rg) {
		affected_row_groups.insert(rg);
	}

	// Row group 1 should be affected (existing data region)
	REQUIRE(affected_row_groups.count(1) > 0);

	auto removed = store->RemoveRowGroupsForTable(context, /*table_oid=*/1, affected_row_groups);
	REQUIRE(removed == 1); // only row group 1 existed in the entry

	auto found = store->Lookup(context, {/*table_oid=*/1, "val = 5"});
	REQUIRE(found != nullptr);
	REQUIRE(found->HasRowGroup(0));
	REQUIRE_FALSE(found->HasRowGroup(1));
}

TEST_CASE("PhysicalCacheInvalidator - invalidation preserves unaffected entries", "[physical_invalidator]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto &context = *con.context;
	auto store = ConditionCacheStore::GetOrCreate(context);

	// Two predicates cached for the same table
	auto entry1 = make_shared_ptr<ConditionCacheEntry>();
	entry1->SetQualifyingVector(/*rg_idx=*/0, /*vec_idx=*/0);
	entry1->SetQualifyingVector(/*rg_idx=*/1, /*vec_idx=*/0);
	store->Upsert(context, {/*table_oid=*/1, "val > 5"}, entry1);

	auto entry2 = make_shared_ptr<ConditionCacheEntry>();
	entry2->SetQualifyingVector(/*rg_idx=*/0, /*vec_idx=*/0);
	entry2->SetQualifyingVector(/*rg_idx=*/1, /*vec_idx=*/0);
	store->Upsert(context, {/*table_oid=*/1, "val < 10"}, entry2);

	// Invalidate row group 0 only
	unordered_set<idx_t> affected_row_groups = {0};
	auto removed = store->RemoveRowGroupsForTable(context, /*table_oid=*/1, affected_row_groups);
	REQUIRE(removed == 2); // one from each entry

	// Both entries should still exist with row group 1 intact
	auto found1 = store->Lookup(context, {/*table_oid=*/1, "val > 5"});
	REQUIRE(found1 != nullptr);
	REQUIRE(found1->HasRowGroup(1));

	auto found2 = store->Lookup(context, {/*table_oid=*/1, "val < 10"});
	REQUIRE(found2 != nullptr);
	REQUIRE(found2->HasRowGroup(1));
}

TEST_CASE("PhysicalCacheInvalidator - full invalidation removes all entries", "[physical_invalidator]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto &context = *con.context;
	auto store = ConditionCacheStore::GetOrCreate(context);

	auto entry = make_shared_ptr<ConditionCacheEntry>();
	entry->SetQualifyingVector(/*rg_idx=*/0, /*vec_idx=*/0);
	entry->SetQualifyingVector(/*rg_idx=*/1, /*vec_idx=*/0);
	store->Upsert(context, {/*table_oid=*/1, "val = 1"}, entry);

	// Invalidate all row groups
	unordered_set<idx_t> all_row_groups = {0, 1};
	store->RemoveRowGroupsForTable(context, /*table_oid=*/1, all_row_groups);

	REQUIRE(store->Lookup(context, {/*table_oid=*/1, "val = 1"}) == nullptr);
	REQUIRE_FALSE(store->HasEntriesForTable(context, /*table_oid=*/1));
}

TEST_CASE("PhysicalCacheInvalidator - row ID to row group mapping", "[physical_invalidator]") {
	// Verify the row_id / DEFAULT_ROW_GROUP_SIZE mapping used by CollectRowGroups
	SECTION("row IDs within same row group map to same index") {
		idx_t rg0_start = 0;
		idx_t rg0_end = DEFAULT_ROW_GROUP_SIZE - 1;
		REQUIRE(rg0_start / DEFAULT_ROW_GROUP_SIZE == 0);
		REQUIRE(rg0_end / DEFAULT_ROW_GROUP_SIZE == 0);
	}

	SECTION("row IDs at boundary map to different row groups") {
		idx_t last_in_rg0 = DEFAULT_ROW_GROUP_SIZE - 1;
		idx_t first_in_rg1 = DEFAULT_ROW_GROUP_SIZE;
		REQUIRE(last_in_rg0 / DEFAULT_ROW_GROUP_SIZE == 0);
		REQUIRE(first_in_rg1 / DEFAULT_ROW_GROUP_SIZE == 1);
	}

	SECTION("large row IDs map correctly") {
		idx_t row_id = DEFAULT_ROW_GROUP_SIZE * 10 + 500;
		REQUIRE(row_id / DEFAULT_ROW_GROUP_SIZE == 10);
	}
}

TEST_CASE("PhysicalCacheInvalidator - INSERT range computation", "[physical_invalidator]") {
	SECTION("insert within single row group") {
		idx_t pre_insert = 100;
		idx_t inserted = 50;
		idx_t first_rg = pre_insert / DEFAULT_ROW_GROUP_SIZE;
		idx_t last_rg = (pre_insert + inserted - 1) / DEFAULT_ROW_GROUP_SIZE;
		REQUIRE(first_rg == 0);
		REQUIRE(last_rg == 0);
	}

	SECTION("insert spanning row group boundary") {
		idx_t pre_insert = DEFAULT_ROW_GROUP_SIZE - 10;
		idx_t inserted = 20;
		idx_t first_rg = pre_insert / DEFAULT_ROW_GROUP_SIZE;
		idx_t last_rg = (pre_insert + inserted - 1) / DEFAULT_ROW_GROUP_SIZE;
		REQUIRE(first_rg == 0);
		REQUIRE(last_rg == 1);
	}

	SECTION("insert spanning multiple row groups") {
		idx_t pre_insert = DEFAULT_ROW_GROUP_SIZE * 2;
		idx_t inserted = DEFAULT_ROW_GROUP_SIZE * 3;
		idx_t first_rg = pre_insert / DEFAULT_ROW_GROUP_SIZE;
		idx_t last_rg = (pre_insert + inserted - 1) / DEFAULT_ROW_GROUP_SIZE;
		REQUIRE(first_rg == 2);
		REQUIRE(last_rg == 4);
	}
}

} // namespace duckdb

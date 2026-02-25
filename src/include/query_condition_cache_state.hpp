#pragma once

#include "duckdb/common/array.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/storage/object_cache.hpp"

namespace duckdb {

// Derived from DuckDB's compile-time configurable constants
inline constexpr idx_t VECTORS_PER_ROW_GROUP = DEFAULT_ROW_GROUP_SIZE / STANDARD_VECTOR_SIZE;
inline constexpr idx_t BITVECTOR_WORDS = (VECTORS_PER_ROW_GROUP + 63) / 64;
static_assert(DEFAULT_ROW_GROUP_SIZE % STANDARD_VECTOR_SIZE == 0,
              "DEFAULT_ROW_GROUP_SIZE must be divisible by STANDARD_VECTOR_SIZE");

// Per row-group bitvector: bit[i] = 1 means vector i has at least one qualifying row,
// for 0 <= i < VECTORS_PER_ROW_GROUP. Callers must not pass out-of-range indices;
// D_ASSERT enforces this in debug builds only.
// Backed by array<uint64_t, N> to support configurable row group / vector sizes.
struct RowGroupBitvector {
	array<uint64_t, BITVECTOR_WORDS> words = {};

	bool VectorHasRows(idx_t vector_index) const;
	void SetVector(idx_t vector_index);
	bool IsEmpty() const;
	idx_t CountSetBits() const;
};

// A single cache entry: the bitvectors for one (table, predicate) combination.
// table_oid is used for lookup within a session.
struct ConditionCacheEntry {
	idx_t table_oid;
	string filter_key;
	unordered_map<idx_t, RowGroupBitvector> bitvectors; // rg_idx -> bitvector
	idx_t total_qualifying_rows = 0;
};

// Stored in DuckDB's per-database ObjectCache
class ConditionCacheStore : public ObjectCacheEntry {
public:
	static constexpr const char *CACHE_KEY = "query_condition_cache_store";

	static string ObjectType() {
		return "query_condition_cache_store";
	}

	string GetObjectType() override {
		return ObjectType();
	}

	// Lookup by filter key
	shared_ptr<ConditionCacheEntry> Lookup(const string &filter_key);

	// Lookup all entries for a table by OID
	vector<shared_ptr<ConditionCacheEntry>> LookupByTable(idx_t table_oid);

	// Insert or update an entry
	void Insert(shared_ptr<ConditionCacheEntry> entry);

	// Clear all entries
	void Clear();

	// Get all entries
	vector<shared_ptr<ConditionCacheEntry>> GetAll();

	// Get or create the store from a client context
	static shared_ptr<ConditionCacheStore> GetOrCreate(ClientContext &context);

private:
	mutex cache_lock;
	// filter_key -> entry
	unordered_map<string, shared_ptr<ConditionCacheEntry>> entries;
};

} // namespace duckdb

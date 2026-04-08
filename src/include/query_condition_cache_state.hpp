#pragma once

#include "concurrency/annotated_lock.hpp"
#include "concurrency/annotated_mutex.hpp"
#include "concurrency/thread_annotation.hpp"

#include "duckdb/common/array.hpp"
#include "duckdb/common/types/hash.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/storage/object_cache.hpp"

namespace duckdb {

// Derived from DuckDB's compile-time configurable constants
inline constexpr idx_t VECTORS_PER_ROW_GROUP = DEFAULT_ROW_GROUP_SIZE / STANDARD_VECTOR_SIZE;
inline constexpr idx_t BITVECTOR_ARRAY_SIZE = (VECTORS_PER_ROW_GROUP + 63) / 64;
static_assert(DEFAULT_ROW_GROUP_SIZE % STANDARD_VECTOR_SIZE == 0,
              "DEFAULT_ROW_GROUP_SIZE must be divisible by STANDARD_VECTOR_SIZE");

// Per row-group bitvector: bit[i] = 1 means vector i has at least one qualifying row,
// for 0 <= i < VECTORS_PER_ROW_GROUP.
// Backed by array<uint64_t, N> to support configurable row group / vector sizes.
struct RowGroupFilter {
	array<uint64_t, BITVECTOR_ARRAY_SIZE> matching_vectors = {};

	RowGroupFilter() = default;

	// Construct from vector indices that contain at least one qualifying row.
	// Each index must be in [0, VECTORS_PER_ROW_GROUP). Duplicates are allowed.
	explicit RowGroupFilter(const vector<idx_t> &qualifying_vectors);

	void SetVector(idx_t vector_index);
	bool VectorHasRows(idx_t vector_index) const;
	bool IsEmpty() const;
	void MergeFrom(const RowGroupFilter &other);
};

// Composite key for cache lookup: (table_oid, filter_key)
struct CacheKey {
	idx_t table_oid;
	string filter_key;

	bool operator==(const CacheKey &other) const {
		return table_oid == other.table_oid && filter_key == other.filter_key;
	}
};

struct CacheKeyHashFunction {
	uint64_t operator()(const CacheKey &key) const {
		return CombineHash(Hash<idx_t>(key.table_oid), Hash(key.filter_key.c_str()));
	}
};

struct CacheEntryStats {
	idx_t qualifying_vectors;
	idx_t total_vectors;
	idx_t qualifying_row_groups;
	idx_t total_row_groups;
};

// A single cache entry: the bitvectors for one (table, predicate) combination.
struct ConditionCacheEntry : public ObjectCacheEntry {
	static string ObjectType() {
		return "query_condition_cache_entry";
	}

	string GetObjectType() override {
		return ObjectType();
	}

	// Return estimated memory usage for LRU eviction
	optional_idx GetEstimatedCacheMemory() const override;

	// Compute statistics about qualifying vectors and row groups
	CacheEntryStats ComputeStats(idx_t total_rows) const;

	// --- Thread-safe API (each method acquires `lock` internally) ---

	// Ensure a row group key exists (empty filter). Used when recording fully excluded row groups.
	void EnsureRowGroup(idx_t rg_idx);
	// Mark that vector `vec_idx` within row group `rg_idx` has at least one qualifying row.
	void SetQualifyingVector(idx_t rg_idx, idx_t vec_idx);
	// Merge another entry's row-group filters into this entry (e.g. after parallel build).
	void MergeFrom(const ConditionCacheEntry &other);

	// Row group absent from cache, or vector has qualifying rows -> predicate may pass rows (matches scan semantics).
	bool VectorPassesFilter(idx_t rg_idx, idx_t vec_idx) const;
	// True iff every row group in [min_rg, max_rg] is present in the cache and has an empty filter.
	bool StatisticsRangeIsAllEmptyCached(idx_t min_rg, idx_t max_rg) const;

	idx_t RowGroupCount() const;
	bool HasRowGroup(idx_t rg_idx) const;
	bool RowGroupVectorHasQualifyingRows(idx_t rg_idx, idx_t vec_idx) const;
	// True iff `rg_idx` is cached and its filter is empty (no qualifying vectors).
	bool RowGroupIsCompletelyEmpty(idx_t rg_idx) const;

	// Erase row group keys; returns (number of keys removed, whether the map is now empty).
	pair<idx_t, bool> EraseRowGroups(const unordered_set<idx_t> &row_group_indices);

private:
	mutable concurrency::mutex lock;
	unordered_map<idx_t, RowGroupFilter> bitvectors DUCKDB_GUARDED_BY(lock); // rg_idx -> bitvector
};

// Per-table index stored in ObjectCache (non-evictable). Tracks which
// filter_keys have cache entries for a given table, enabling fast
// invalidation lookup when DML modifies the table.
struct TableFilterKeyIndex : public ObjectCacheEntry {
	concurrency::mutex lock;
	// Keys that have been cached for this table; entries may have been evicted by LRU.
	// Absence of a key guarantees it is not cached.
	unordered_set<string> filter_keys DUCKDB_GUARDED_BY(lock);

	static string ObjectType() {
		return "query_condition_cache_filter_key_index";
	}

	string GetObjectType() override {
		return ObjectType();
	}

	optional_idx GetEstimatedCacheMemory() const override {
		return optional_idx {};
	}

	// Add a filter key. No-op if it already exists.
	void Add(const string &filter_key);
	// Remove a filter key. Assumes the key must appear in the set.
	void Remove(const string &filter_key);
	bool IsEmpty();
	// Invoke callback for each filter key while holding the lock.
	template <typename Fn>
	void ForEach(Fn &&fn) {
		concurrency::lock_guard<concurrency::mutex> guard(lock);
		for (const auto &key : filter_keys) {
			fn(key);
		}
	}
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

	optional_idx GetEstimatedCacheMemory() const override {
		return optional_idx {};
	}

	// Lookup by cache key; returns nullptr if not found
	shared_ptr<ConditionCacheEntry> Lookup(ClientContext &context, const CacheKey &key);

	// Upsert an entry
	void Upsert(ClientContext &context, const CacheKey &key, shared_ptr<ConditionCacheEntry> entry);

	// Remove specific row groups from all entries for a table. Returns count of row groups removed.
	idx_t RemoveRowGroupsForTable(ClientContext &context, idx_t table_oid,
	                              const unordered_set<idx_t> &row_group_indices);

	// Check if any entries exist for a given table OID
	bool HasEntriesForTable(ClientContext &context, idx_t table_oid);

	// Clear all cache entries and filter key indices
	void ClearAll(ClientContext &context);

	// Get or create the store from a client context
	static shared_ptr<ConditionCacheStore> GetOrCreate(ClientContext &context);

private:
	concurrency::mutex lock;
	// Tracks all table OIDs that have been cached, for ClearAll
	unordered_set<idx_t> cached_table_oids DUCKDB_GUARDED_BY(lock);

	static string MakeCacheKeyString(const CacheKey &key);
	static string MakeFilterKeyIndexKey(idx_t table_oid);
};

} // namespace duckdb

#pragma once

#include "concurrency/annotated_lock.hpp"
#include "concurrency/annotated_mutex.hpp"
#include "concurrency/thread_annotation.hpp"

#include <bitset>

#include "duckdb/common/atomic.hpp"
#include "duckdb/common/types/hash.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/storage/object_cache.hpp"

namespace duckdb {

// Derived from DuckDB's compile-time configurable constants
inline constexpr idx_t VECTORS_PER_ROW_GROUP = DEFAULT_ROW_GROUP_SIZE / STANDARD_VECTOR_SIZE;
static_assert(DEFAULT_ROW_GROUP_SIZE % STANDARD_VECTOR_SIZE == 0,
              "DEFAULT_ROW_GROUP_SIZE must be divisible by STANDARD_VECTOR_SIZE");

// Per row-group pair of bitsets:
//   matching_vectors[i] = 1 iff vec i has at least one qualifying row.
//   observed[i]         = 1 iff vec i has been observed by an exact scan path.
struct RowGroupFilter {
	std::bitset<VECTORS_PER_ROW_GROUP> matching_vectors;
	std::bitset<VECTORS_PER_ROW_GROUP> observed;

	RowGroupFilter() = default;

	// Leaves observed all-zero; call SetObserved explicitly when a vec was scanned.
	explicit RowGroupFilter(const vector<idx_t> &qualifying_vectors);

	void SetVector(idx_t vector_index);
	bool VectorHasRows(idx_t vector_index) const;
	bool IsEmpty() const;
	void SetObserved(idx_t vector_index);
	void MarkFullyObserved();
	bool IsObserved(idx_t vector_index) const;
	bool IsFullyObserved() const;
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
	idx_t cached_row_groups;
	idx_t total_row_groups;
};

struct CacheObservationRange {
	idx_t start_row;
	idx_t count;
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

	// Compute statistics about qualifying vectors and cached row groups
	CacheEntryStats ComputeStats(idx_t total_rows) const;

	// --- Thread-safe API (each method acquires `lock` internally) ---

	// Ensure a row group key exists (empty filter). Used when recording fully excluded row groups.
	void EnsureRowGroup(idx_t rg_idx);
	// Mark that vector `vec_idx` within row group `rg_idx` has at least one qualifying row.
	void SetQualifyingVector(idx_t rg_idx, idx_t vec_idx);
	// Merge another entry's row-group filters into this entry (e.g. after parallel build).
	void MergeFrom(const ConditionCacheEntry &other);
	// Used by full-table builds to declare every row group fully observed.
	void MarkAllRowGroupsFullyObserved();
	void MarkRowGroupFullyObserved(idx_t rg_idx);
	// Mark that a specific vector was observed by an exact scan path.
	void SetObservedVector(idx_t rg_idx, idx_t vec_idx);

	// Row group absent from cache, or vector not observed yet, or vector has qualifying rows -> pass rows through.
	bool VectorPassesFilter(idx_t rg_idx, idx_t vec_idx) const;
	// True iff every row group in [min_rg, max_rg] is present in the cache, empty, and fully observed.
	bool StatisticsRangeIsAllEmptyCached(idx_t min_rg, idx_t max_rg) const;
	// True iff some row group/vector covering [0, total_rows) is absent or not fully observed.
	bool NeedsObservation(idx_t total_rows) const;
	vector<CacheObservationRange> GetUnobservedVectorRanges(idx_t total_rows) const;

	idx_t RowGroupCount() const;
	bool HasRowGroup(idx_t rg_idx) const;
	idx_t GetObservedVectorCount(idx_t rg_idx) const;
	bool RowGroupVectorHasQualifyingRows(idx_t rg_idx, idx_t vec_idx) const;
	// True iff `rg_idx` is cached and its filter is empty (no qualifying vectors).
	bool RowGroupIsCompletelyEmpty(idx_t rg_idx) const;

	// Erase row group keys; returns (number of keys removed, whether the map is now empty).
	pair<idx_t, bool> EraseRowGroups(const unordered_set<idx_t> &row_group_indices);
	pair<idx_t, bool> EraseRowGroupsStartingAt(idx_t first_row_group);

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
	// Transfer ownership of all filter keys out. Clears the internal set.
	unordered_set<string> Take();
	// Return a copy of all filter keys without clearing the set.
	unordered_set<string> Snapshot();
};

struct CacheStoreStats {
	idx_t total_memory_bytes;
	idx_t hit_count;
	idx_t access_count;
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
	idx_t RemoveRowGroupsStartingAtForTable(ClientContext &context, idx_t table_oid, idx_t first_row_group);
	idx_t RemoveRowGroupsStartingAtForTable(DatabaseInstance &db, idx_t table_oid, idx_t first_row_group);

	// Check if any entries exist for a given table OID
	bool HasEntriesForTable(ClientContext &context, idx_t table_oid);

	// Clear all cache entries and filter key indices
	void ClearAll(ClientContext &context);

	// Get or create the store from a client context
	static shared_ptr<ConditionCacheStore> GetOrCreate(ClientContext &context);
	static shared_ptr<ConditionCacheStore> GetOrCreate(DatabaseInstance &db);

	// Record an optimizer lookup attempt.
	void RecordAccess(bool hit);

	// Reset cache access stats.
	void ResetStats();

	// Compute the sum of estimated memory used by all live cache entries.
	idx_t ComputeTotalMemoryBytes(ClientContext &context) const;

	// Return a snapshot of current stats.
	CacheStoreStats GetStats(ClientContext &context) const;

private:
	mutable concurrency::mutex lock;
	// Tracks all table OIDs that have been cached, for ClearAll
	unordered_set<idx_t> cached_table_oids DUCKDB_GUARDED_BY(lock);

	atomic<idx_t> total_accesses {0};
	atomic<idx_t> total_hits {0};

	static string MakeCacheKeyString(const CacheKey &key);
	static string MakeFilterKeyIndexKey(idx_t table_oid);
};

} // namespace duckdb

#pragma once

#include "concurrency/annotated_lock.hpp"
#include "concurrency/annotated_mutex.hpp"
#include "concurrency/thread_annotation.hpp"

#include <bitset>

#include "duckdb/common/types/hash.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/storage/object_cache.hpp"

namespace duckdb {

// Derived from DuckDB's compile-time configurable constants
inline constexpr idx_t VECTORS_PER_ROW_GROUP = DEFAULT_ROW_GROUP_SIZE / STANDARD_VECTOR_SIZE;
static_assert(DEFAULT_ROW_GROUP_SIZE % STANDARD_VECTOR_SIZE == 0,
              "DEFAULT_ROW_GROUP_SIZE must be divisible by STANDARD_VECTOR_SIZE");

// Per row-group pair of bitsets.
//   matching_vectors[i] = 1 iff vec i observed to have at least one qualifying row.
//   observed[i]         = 1 iff vec i has been observed. An unobserved vec is
//                         "unknown" and must be scanned.
struct RowGroupFilter {
	std::bitset<VECTORS_PER_ROW_GROUP> matching_vectors;
	std::bitset<VECTORS_PER_ROW_GROUP> observed;

	RowGroupFilter() = default;

	// Leaves observed all-zero; set bits explicitly if you need pruning semantics.
	explicit RowGroupFilter(const vector<idx_t> &qualifying_vectors);

	void SetVector(idx_t vector_index);
	bool VectorHasRows(idx_t vector_index) const;
	bool IsEmpty() const;
	void SetObserved(idx_t vector_index);
	bool IsObserved(idx_t vector_index) const;
	bool IsFullyObserved() const;
	// OR-merge of both bitsets.
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

	// Ensure a row group key exists (empty matching, no observed bits set).
	void EnsureRowGroup(idx_t rg_idx);
	void SetQualifyingVector(idx_t rg_idx, idx_t vec_idx);
	void MergeFrom(const ConditionCacheEntry &other);
	// Used by manual full-table builds to declare every keyed rg fully observed.
	void MarkAllRowGroupsFullyObserved();
	// Mark a single vec as observed. Safe per-task because store-side MergeFrom ORs observed.
	void SetObservedVector(idx_t rg_idx, idx_t vec_idx);

	// False only if rg is cached AND vec is observed AND matching bit = 0.
	bool VectorPassesFilter(idx_t rg_idx, idx_t vec_idx) const;
	// True iff every rg in range is cached, fully observed, and empty.
	bool StatisticsRangeIsAllEmptyCached(idx_t min_rg, idx_t max_rg) const;

	idx_t RowGroupCount() const;
	bool HasRowGroup(idx_t rg_idx) const;
	// Popcount of the rg's observed bitmask; 0 if rg is not present.
	idx_t GetObservedVectorCount(idx_t rg_idx) const;
	// Raw matching bit, ignores observed. Test-only; production uses VectorPassesFilter.
	bool RowGroupVectorHasQualifyingRows(idx_t rg_idx, idx_t vec_idx) const;
	// Raw emptiness of matching bits, ignores observed. Test-only.
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
	// Transfer ownership of all filter keys out. Clears the internal set.
	unordered_set<string> Take();
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

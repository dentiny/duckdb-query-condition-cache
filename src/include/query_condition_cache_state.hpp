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

	optional_idx GetEstimatedCacheMemory() const override;
	CacheEntryStats ComputeStats(idx_t total_rows) const;

	// Thread-safe API (each method acquires `lock` internally).
	void EnsureRowGroup(idx_t rg_idx);
	void SetQualifyingVector(idx_t rg_idx, idx_t vec_idx);
	void MergeFrom(const ConditionCacheEntry &other);

	bool VectorPassesFilter(idx_t rg_idx, idx_t vec_idx) const;
	bool StatisticsRangeIsAllEmptyCached(idx_t min_rg, idx_t max_rg) const;

	idx_t RowGroupCount() const;
	bool HasRowGroup(idx_t rg_idx) const;
	bool RowGroupVectorHasQualifyingRows(idx_t rg_idx, idx_t vec_idx) const;
	bool RowGroupIsCompletelyEmpty(idx_t rg_idx) const;

	pair<idx_t, bool> EraseRowGroups(const unordered_set<idx_t> &row_group_indices);

private:
	mutable concurrency::mutex lock;
	unordered_map<idx_t, RowGroupFilter> bitvectors DUCKDB_GUARDED_BY(lock);
};

// Per-table index stored in ObjectCache (non-evictable). Tracks which
// filter_keys have cache entries for a given table, enabling fast
// invalidation lookup when DML modifies the table.
struct TableFilterKeyIndex : public ObjectCacheEntry {
	concurrency::mutex lock;
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

	void Add(const string &filter_key);
	void Remove(const string &filter_key);
	bool IsEmpty();
	unordered_set<string> GetAll();
};

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

	shared_ptr<ConditionCacheEntry> Lookup(ClientContext &context, const CacheKey &key);
	void Upsert(ClientContext &context, const CacheKey &key, shared_ptr<ConditionCacheEntry> entry);
	idx_t RemoveRowGroupsForTable(ClientContext &context, idx_t table_oid,
	                              const unordered_set<idx_t> &row_group_indices);
	bool HasEntriesForTable(ClientContext &context, idx_t table_oid);
	void ClearAll(ClientContext &context);
	static shared_ptr<ConditionCacheStore> GetOrCreate(ClientContext &context);

private:
	concurrency::mutex lock;
	unordered_set<idx_t> cached_table_oids DUCKDB_GUARDED_BY(lock);

	static string MakeCacheKeyString(const CacheKey &key);
	static string MakeFilterKeyIndexKey(idx_t table_oid);
};

} // namespace duckdb

#pragma once

#include "duckdb/common/array.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/types/hash.hpp"
#include "duckdb/common/unordered_map.hpp"
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

// A single cache entry: the bitvectors for one (table, predicate) combination.
struct ConditionCacheEntry : public ObjectCacheEntry {
	unordered_map<idx_t, RowGroupFilter> bitvectors; // rg_idx -> bitvector

	static string ObjectType() {
		return "query_condition_cache_entry";
	}

	string GetObjectType() override {
		return ObjectType();
	}

	// Return estimated memory usage for LRU eviction
	optional_idx GetEstimatedCacheMemory() const override;
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

	// Get or create the store from a client context
	static shared_ptr<ConditionCacheStore> GetOrCreate(ClientContext &context);

private:
	// Generate a unique cache key string from CacheKey
	static string MakeCacheKeyString(const CacheKey &key);
};

} // namespace duckdb

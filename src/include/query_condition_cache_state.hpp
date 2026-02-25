#pragma once

#include "duckdb/common/array.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/storage/object_cache.hpp"

namespace duckdb {

// Derived from DuckDB's compile-time configurable constants
static constexpr idx_t VECTORS_PER_ROW_GROUP = DEFAULT_ROW_GROUP_SIZE / STANDARD_VECTOR_SIZE;
static constexpr idx_t BITVECTOR_WORDS = (VECTORS_PER_ROW_GROUP + 63) / 64;

// Per row-group bitvector: bit[i] = 1 means vector i has at least one qualifying row.
// Backed by array<uint64_t, N> to support configurable row group / vector sizes.
struct RowGroupBitvector {
	array<uint64_t, BITVECTOR_WORDS> words = {};

	bool VectorHasRows(idx_t vector_index) const {
		D_ASSERT(vector_index < VECTORS_PER_ROW_GROUP);
		return (words[vector_index / 64] >> (vector_index % 64)) & 1ULL;
	}

	void SetVector(idx_t vector_index) {
		D_ASSERT(vector_index < VECTORS_PER_ROW_GROUP);
		words[vector_index / 64] |= (1ULL << (vector_index % 64));
	}

	bool IsEmpty() const {
		for (auto &w : words) {
			if (w != 0) {
				return false;
			}
		}
		return true;
	}

	idx_t CountSetBits() const {
		idx_t count = 0;
		for (auto &w : words) {
			uint64_t v = w;
			while (v) {
				count += v & 1ULL;
				v >>= 1;
			}
		}
		return count;
	}
};

// A single cache entry: the bitvectors for one (table, predicate) combination
struct ConditionCacheEntry {
	string table_name;
	string filter_key;
	unordered_map<idx_t, RowGroupBitvector> bitvectors; // rg_idx -> bitvector
	idx_t total_qualifying_rows = 0;
};

// Thread-local state for capturing filter key during query_condition_cache_build
struct BuildCaptureState {
	bool active = false;
	string target_table;
	string captured_filter_key;
};

// TODO: Replace thread_local variables with ClientContextState to avoid non-trivial thread_local types.

// Global thread-local build capture state
extern thread_local BuildCaptureState tl_build_capture;

// Pre-optimize match: table_index -> cache entry to inject in the post-optimize pass.
// Populated by PreOptimizeFunction, consumed and cleared by OptimizeFunction.
extern thread_local unordered_map<idx_t, shared_ptr<ConditionCacheEntry>> tl_lookup_pending;

// Stored in DuckDB's per-database ObjectCache, non-evictable
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

	// Lookup all entries for a table
	vector<shared_ptr<ConditionCacheEntry>> LookupByTable(const string &table_name);

	// Insert an entry
	void Insert(shared_ptr<ConditionCacheEntry> entry);

	// Clear all entries
	void Clear();

	// Get all entries (for query_condition_cache_info)
	vector<shared_ptr<ConditionCacheEntry>> GetAll();

	// Get or create the store from a client context
	static shared_ptr<ConditionCacheStore> GetOrCreate(ClientContext &context);

private:
	mutex cache_lock;
	// filter_key -> entry
	unordered_map<string, shared_ptr<ConditionCacheEntry>> entries;
};

} // namespace duckdb

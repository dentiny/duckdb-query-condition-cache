#include "query_condition_cache_state.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/storage/object_cache.hpp"

namespace duckdb {

// ------- ROW_GROUP_FILTER -------

RowGroupFilter::RowGroupFilter(const vector<idx_t> &qualifying_vectors) {
	for (const auto &vec_idx : qualifying_vectors) {
		matching_vectors.at(vec_idx / 64) |= (1ULL << (vec_idx % 64));
	}
}

void RowGroupFilter::SetVector(idx_t vector_index) {
	matching_vectors.at(vector_index / 64) |= (1ULL << (vector_index % 64));
}

bool RowGroupFilter::VectorHasRows(idx_t vector_index) const {
	return (matching_vectors.at(vector_index / 64) >> (vector_index % 64)) & 1ULL;
}

bool RowGroupFilter::IsEmpty() const {
	for (const auto &w : matching_vectors) {
		if (w != 0) {
			return false;
		}
	}
	return true;
}

void RowGroupFilter::MergeFrom(const RowGroupFilter &other) {
	for (idx_t i = 0; i < BITVECTOR_ARRAY_SIZE; ++i) {
		matching_vectors[i] |= other.matching_vectors[i];
	}
}

// ------- CONDITION_CACHE_ENTRY -------

optional_idx ConditionCacheEntry::GetEstimatedCacheMemory() const {
	// Rough estimate: each RowGroupFilter is ~BITVECTOR_ARRAY_SIZE * 8 bytes
	// Plus overhead for the map structure
	idx_t estimated_size = sizeof(ConditionCacheEntry);
	estimated_size += bitvectors.size() * (sizeof(idx_t) + sizeof(RowGroupFilter) + 32); // map overhead
	return optional_idx(estimated_size);
}

CacheEntryStats ConditionCacheEntry::ComputeStats(idx_t total_rows) const {
	constexpr idx_t vectors_per_row_group = DEFAULT_ROW_GROUP_SIZE / STANDARD_VECTOR_SIZE;

	idx_t qualifying_vectors = 0;
	idx_t qualifying_row_groups = 0;
	for (const auto &[rg_idx, filter] : bitvectors) {
		if (!filter.IsEmpty()) {
			++qualifying_row_groups;
		}
		for (idx_t v = 0; v < vectors_per_row_group; ++v) {
			if (filter.VectorHasRows(v)) {
				++qualifying_vectors;
			}
		}
	}

	idx_t full_row_groups = total_rows / DEFAULT_ROW_GROUP_SIZE;
	idx_t remaining_rows = total_rows % DEFAULT_ROW_GROUP_SIZE;
	idx_t total_vectors = full_row_groups * vectors_per_row_group;
	if (remaining_rows > 0) {
		total_vectors += (remaining_rows + STANDARD_VECTOR_SIZE - 1) / STANDARD_VECTOR_SIZE;
	}
	idx_t total_row_groups = (total_rows + DEFAULT_ROW_GROUP_SIZE - 1) / DEFAULT_ROW_GROUP_SIZE;

	return CacheEntryStats {
	    .qualifying_vectors = qualifying_vectors,
	    .total_vectors = total_vectors,
	    .qualifying_row_groups = qualifying_row_groups,
	    .total_row_groups = total_row_groups,
	};
}

// ------- CONDITION_CACHE_STORE -------

string ConditionCacheStore::MakeCacheKeyString(const CacheKey &key) {
	// Format: "query_condition_cache_entry:{table_oid}:{filter_key}"
	return StringUtil::Format("query_condition_cache_entry:%llu:%s", key.table_oid, key.filter_key);
}

shared_ptr<ConditionCacheEntry> ConditionCacheStore::Lookup(ClientContext &context, const CacheKey &key) {
	auto &cache = ObjectCache::GetObjectCache(context);
	string cache_key = MakeCacheKeyString(key);
	return cache.Get<ConditionCacheEntry>(cache_key);
}

void ConditionCacheStore::Upsert(ClientContext &context, const CacheKey &key, shared_ptr<ConditionCacheEntry> entry) {
	if (!entry) {
		throw InvalidInputException("ConditionCacheStore::Upsert: entry must not be null");
	}
	auto &cache = ObjectCache::GetObjectCache(context);
	string cache_key = MakeCacheKeyString(key);
	{
		const lock_guard<mutex> guard(store_mutex);
		tracked_keys.insert(cache_key);
	}
	cache.Put(cache_key, std::move(entry));
}

void ConditionCacheStore::ClearAll(ClientContext &context) {
	auto &cache = ObjectCache::GetObjectCache(context);
	const lock_guard<mutex> guard(store_mutex);
	for (const auto &key : tracked_keys) {
		cache.Delete(key);
	}
	tracked_keys.clear();
}

shared_ptr<ConditionCacheStore> ConditionCacheStore::GetOrCreate(ClientContext &context) {
	auto &cache = ObjectCache::GetObjectCache(context);
	return cache.GetOrCreate<ConditionCacheStore>(CACHE_KEY);
}

bool ConditionCacheStore::IsEnabled(ClientContext &context) {
	Value val;
	if (context.TryGetCurrentSetting("enable_query_condition_cache", val)) {
		return val.GetValue<bool>();
	}
	return true;
}

} // namespace duckdb

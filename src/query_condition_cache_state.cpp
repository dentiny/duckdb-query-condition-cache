#include "query_condition_cache_state.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
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

// ------- CONDITION_CACHE_ENTRY -------

optional_idx ConditionCacheEntry::GetEstimatedCacheMemory() const {
	// Rough estimate: each RowGroupFilter is ~BITVECTOR_ARRAY_SIZE * 8 bytes
	// Plus overhead for the map structure
	idx_t estimated_size = sizeof(ConditionCacheEntry);
	estimated_size += bitvectors.size() * (sizeof(idx_t) + sizeof(RowGroupFilter) + 32); // map overhead
	return optional_idx(estimated_size);
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
	cache.Put(cache_key, std::move(entry));
}

idx_t ConditionCacheStore::RemoveRowGroupsForTable(idx_t table_oid, const unordered_set<idx_t> &row_group_indices) {
	lock_guard<mutex> guard(cache_lock);
	idx_t removed_count = 0;
	vector<CacheKey> keys_to_remove;
	for (auto &pair : entries) {
		if (pair.first.table_oid != table_oid) {
			continue;
		}
		auto &entry = pair.second;
		for (auto rg_idx : row_group_indices) {
			if (entry->bitvectors.erase(rg_idx) > 0) {
				++removed_count;
			}
		}
		if (entry->bitvectors.empty()) {
			keys_to_remove.push_back(pair.first);
		}
	}
	for (auto &key : keys_to_remove) {
		entries.erase(key);
	}
	return removed_count;
}

shared_ptr<ConditionCacheStore> ConditionCacheStore::GetOrCreate(ClientContext &context) {
	auto &cache = ObjectCache::GetObjectCache(context);
	return cache.GetOrCreate<ConditionCacheStore>(CACHE_KEY);
}

} // namespace duckdb

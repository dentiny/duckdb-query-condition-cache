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

// ------- TABLE_FILTER_KEY_INDEX -------

void TableFilterKeyIndex::Add(const string &filter_key) {
	concurrency::lock_guard<concurrency::mutex> guard(lock);
	for (const auto &existing : filter_keys) {
		if (existing == filter_key) {
			return;
		}
	}
	filter_keys.push_back(filter_key);
}

void TableFilterKeyIndex::Remove(const string &filter_key) {
	concurrency::lock_guard<concurrency::mutex> guard(lock);
	for (auto it = filter_keys.begin(); it != filter_keys.end(); ++it) {
		if (*it == filter_key) {
			filter_keys.erase(it);
			return;
		}
	}
}

vector<string> TableFilterKeyIndex::GetAll() {
	concurrency::lock_guard<concurrency::mutex> guard(lock);
	return filter_keys;
}

// ------- CONDITION_CACHE_STORE -------

// Format: "query_condition_cache_entry:{table_oid}:{filter_key}"
string ConditionCacheStore::MakeCacheKeyString(const CacheKey &key) {
	return StringUtil::Format("query_condition_cache_entry:%llu:%s", key.table_oid, key.filter_key);
}

string ConditionCacheStore::MakeFilterKeyIndexKey(idx_t table_oid) {
	return StringUtil::Format("query_condition_cache_filter_key_index:%llu", table_oid);
}

shared_ptr<ConditionCacheEntry> ConditionCacheStore::Lookup(ClientContext &context, const CacheKey &key) {
	auto &cache = ObjectCache::GetObjectCache(context);
	return cache.Get<ConditionCacheEntry>(MakeCacheKeyString(key));
}

void ConditionCacheStore::Upsert(ClientContext &context, const CacheKey &key, shared_ptr<ConditionCacheEntry> entry) {
	if (!entry) {
		throw InvalidInputException("ConditionCacheStore::Upsert: entry must not be null");
	}
	auto &cache = ObjectCache::GetObjectCache(context);
	cache.Put(MakeCacheKeyString(key), std::move(entry));

	// Maintain per-table index for invalidation lookup
	auto index = cache.GetOrCreate<TableFilterKeyIndex>(MakeFilterKeyIndexKey(key.table_oid));
	index->Add(key.filter_key);

	// Track table OID for ClearAll
	concurrency::lock_guard<concurrency::mutex> guard(lock);
	cached_table_oids.insert(key.table_oid);
}

idx_t ConditionCacheStore::RemoveRowGroupsForTable(ClientContext &context, idx_t table_oid,
                                                   const unordered_set<idx_t> &row_group_indices) {
	auto &cache = ObjectCache::GetObjectCache(context);

	auto index = cache.Get<TableFilterKeyIndex>(MakeFilterKeyIndexKey(table_oid));
	if (!index) {
		return 0;
	}

	auto filter_keys = index->GetAll();
	idx_t removed_count = 0;

	for (const auto &filter_key : filter_keys) {
		CacheKey key {table_oid, filter_key};
		string cache_key = MakeCacheKeyString(key);
		auto entry = cache.Get<ConditionCacheEntry>(cache_key);
		if (!entry) {
			index->Remove(filter_key);
			continue;
		}
		for (auto rg_idx : row_group_indices) {
			if (entry->bitvectors.erase(rg_idx) > 0) {
				++removed_count;
			}
		}
		if (entry->bitvectors.empty()) {
			cache.Delete(cache_key);
			index->Remove(filter_key);
		}
	}
	return removed_count;
}

bool ConditionCacheStore::HasEntriesForTable(ClientContext &context, idx_t table_oid) {
	auto &cache = ObjectCache::GetObjectCache(context);
	auto index = cache.Get<TableFilterKeyIndex>(MakeFilterKeyIndexKey(table_oid));
	return index && !index->GetAll().empty();
}

void ConditionCacheStore::ClearAll(ClientContext &context) {
	auto &cache = ObjectCache::GetObjectCache(context);

	concurrency::lock_guard<concurrency::mutex> guard(lock);
	for (auto table_oid : cached_table_oids) {
		auto index = cache.Get<TableFilterKeyIndex>(MakeFilterKeyIndexKey(table_oid));
		if (!index) {
			continue;
		}
		auto filter_keys = index->GetAll();
		for (const auto &filter_key : filter_keys) {
			cache.Delete(MakeCacheKeyString(CacheKey {table_oid, filter_key}));
		}
		cache.Delete(MakeFilterKeyIndexKey(table_oid));
	}
	cached_table_oids.clear();
}

shared_ptr<ConditionCacheStore> ConditionCacheStore::GetOrCreate(ClientContext &context) {
	auto &cache = ObjectCache::GetObjectCache(context);
	return cache.GetOrCreate<ConditionCacheStore>(CACHE_KEY);
}

} // namespace duckdb

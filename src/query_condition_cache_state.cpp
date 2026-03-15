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
bool RowGroupFilter::IsFull() const {
	// Check if all VECTORS_PER_ROW_GROUP bits are set
	for (idx_t i = 0; i < BITVECTOR_ARRAY_SIZE; i++) {
		idx_t remaining = VECTORS_PER_ROW_GROUP - i * 64;
		uint64_t expected;
		if (remaining >= 64) {
			expected = ~uint64_t(0);
		} else {
			expected = (1ULL << remaining) - 1;
		}
		if (matching_vectors[i] != expected) {
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

// ------- TABLE_FILTER_KEY_INDEX -------

void TableFilterKeyIndex::Add(const string &filter_key) {
	lock_guard<mutex> guard(lock);
	for (auto &existing : filter_keys) {
		if (existing == filter_key) {
			return;
		}
	}
	filter_keys.push_back(filter_key);
}

void TableFilterKeyIndex::Remove(const string &filter_key) {
	lock_guard<mutex> guard(lock);
	for (auto it = filter_keys.begin(); it != filter_keys.end(); ++it) {
		if (*it == filter_key) {
			filter_keys.erase(it);
			return;
		}
	}
}

vector<string> TableFilterKeyIndex::GetAll() {
	lock_guard<mutex> guard(lock);
	return filter_keys;
}

// ------- CONDITION_CACHE_STORE -------

string ConditionCacheStore::MakeCacheKeyString(const CacheKey &key) {
	return StringUtil::Format("qcc_entry:%llu:%s", key.table_oid, key.filter_key);
}

string ConditionCacheStore::MakeFilterKeyIndexKey(idx_t table_oid) {
	return StringUtil::Format("qcc_filter_key_index:%llu", table_oid);
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

	auto index = cache.GetOrCreate<TableFilterKeyIndex>(MakeFilterKeyIndexKey(key.table_oid));
	index->Add(key.filter_key);
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

	for (auto &filter_key : filter_keys) {
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

shared_ptr<ConditionCacheStore> ConditionCacheStore::GetOrCreate(ClientContext &context) {
	auto &cache = ObjectCache::GetObjectCache(context);
	return cache.GetOrCreate<ConditionCacheStore>(CACHE_KEY);
}

} // namespace duckdb

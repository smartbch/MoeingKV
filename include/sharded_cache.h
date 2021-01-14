#pragma once
#include <mutex>
#include <atomic>
#include "xxhash64.h"
#include "common.h"
#include "cpp-btree-1.0.1/btree_map.h"

namespace moeingkv {

// A cache with N shards. Each shard has its own mutex and allows one accessor for one time.
// With N shards, this cache can have many accessors who are accessing different shard.
// It caches the KV pairs contained in the on-disk and in-mem vaults. Each cache entry has a 
// timestamp to indicate its age. When a cache shard is full, we try to find an old enough
// entry to evict.
template<int N>
class sharded_cache {
	enum {
		EVICT_TRY_DIST = 10,
	};
	struct dstr_id_time {
		std::string kstr;
		std::string vstr;
		int64_t     id;
		int64_t     timestamp;
	};
	typedef btree::btree_multimap<uint64_t, dstr_id_time> i2str_map;
	struct map {
		i2str_map  m;
		std::mutex mtx;
		void lock() { 
			//since each access to map would not take a long time, we keep waiting here
			while(!mtx.try_lock()) {/*do nothing*/}
		}
		void unlock() {
			mtx.unlock();
		}
		size_t size() {
			return m.size();
		}
		// Look up the corresponding 'str_with_id' for 'kstr'. 'key' must be short hash of 'key_str'
		// Returns whether a valid 'out' is found.
		bool lookup(uint64_t key, const std::string& kstr, str_with_id* out_ptr) {
			lock();
			bool res = false;
			for(auto iter = m.find(key); iter != m.end(); iter++) {
				if(iter->second.kstr == kstr) {
					out_ptr->str = iter->second.vstr;
					out_ptr->id = iter->second.id;
					res = true;
					break;
				}
			}
			unlock();
			return res;
		}
		// Insert a new entry to the cache or change the cached value, to keep sync with the vaults.
		void add(uint64_t key, const dstr_id_time& value) {
			lock();
			bool found_it = false;
			for(auto iter = m.find(key); iter != m.end(); iter++) {
				if(iter->second.kstr == value.kstr) {
					iter->second = value;
					found_it = true;
					break;
				}
			}
			if(!found_it) {
				m.insert(std::make_pair(key, value));
			}
			unlock();
		}
		// Start iterating from rand_key for EVICT_TRY_DIST steps to find an oldest entry and evict it
		void evict_oldest(uint64_t rand_key) {
			lock();
			auto del_pos = m.end(); //pointing to the position for eviction
			int64_t smallest_time = -1;
			int dist = 0;
			for(auto iter = m.lower_bound(rand_key); iter != m.end(); iter++) {
				if(dist++ > EVICT_TRY_DIST) break;
				if(smallest_time == -1 || smallest_time > iter->second.timestamp) {
					del_pos = iter;
					smallest_time = iter->second.timestamp;
				}
			}
			if(del_pos != m.end()) {
				m.erase(del_pos);
			}
			unlock();
		}
	};
public:
	map                map_arr[N];
	int64_t            timestamp;
	std::atomic_ullong rand_key;
	size_t             shard_max_size;

	void set_timestamp(int64_t t) {
		timestamp = t;
	}
	void set_shard_max_size(size_t sz) {
		shard_max_size = sz;
	}
	// lookup a cache entry. 'key' must be short hash of 'key_str'
	bool lookup(uint64_t key, const std::string& key_str, str_with_id* out_ptr) {
		rand_key.fetch_xor(key);
		return map_arr[key%N].lookup(key, key_str, out_ptr);
	}
	// Add a new cache entry and if the shard is full, evict the oldest
	void add(uint64_t key, const std::string& kstr, const std::string& vstr, int64_t id) {
		auto value = dstr_id_time{.kstr=kstr, .vstr=vstr, .id=id, .timestamp=timestamp};
		auto idx = key%N;
		if(map_arr[idx].size() > shard_max_size) {
			rand_key.store(hash(rand_key.load(), key));
			map_arr[idx].evict_oldest(rand_key.load());
		}
		map_arr[idx].add(key, value);
	}
};

//unsigned long milliseconds_since_epoch =
//    std::chrono::system_clock::now().time_since_epoch() / 
//    std::chrono::milliseconds(1);

}

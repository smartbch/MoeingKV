#pragma once
#include <mutex>
#include <atomic>
#include "xxhash64.h"
#include "common.h"
#include "cpp-btree-1.0.1/btree_map.h"

namespace moeingkv {

inline uint64_t hashstr(const std::string& str, uint64_t seed) {
	return XXHash64::hash(str.data(), str.size(), seed);
}

template<int N>
class sharded_map {
	enum {
		EVICT_DRY_DIST = 10,
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
			while(!mtx.try_lock()) {/*do nothing*/}
		}
		void unlock() {
			mtx.unlock();
		}
		size_t size() {
			return m.size();
		}
		bool get(uint64_t key, const std::string& kstr, str_with_id* out_ptr) {
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
		void mark_as_deleted(uint64_t key, const std::string& kstr) {
			lock();
			for(auto iter = m.find(key); iter != m.end(); iter++) {
				if(iter->second.kstr == kstr) {
					iter->second.id = -1;
					break;
				}
			}
			unlock();
		}
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
		void evict_oldest(uint64_t rand_key) {
			lock();
			auto del_pos = m.end();
			int64_t timestamp = -1;
			int dist = 0;
			for(auto iter = m.lower_bound(rand_key); iter != m.end(); iter++) {
				if(dist++ > EVICT_DRY_DIST) break;
				if(timestamp == -1 || timestamp > iter->second.timestamp) {
					del_pos = iter;
					timestamp = iter->second.timestamp;
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
	bool get(uint64_t key, const std::string& kstr, str_with_id* out_ptr) {
		rand_key.fetch_xor(key);
		return map_arr[key%N].get(key, kstr, out_ptr);
	}
	void add(uint64_t key, const std::string& kstr, const std::string& vstr, int64_t id) {
		auto value = dstr_id_time{.kstr=kstr, .vstr=vstr, .id=id, .timestamp=timestamp};
		if(map_arr[key%N].size() > shard_max_size) {
			map_arr[key%N].evict_oldest(rand_key.load());
		}
		map_arr[key%N].add(key, value);
	}
	void mark_as_deleted(uint64_t key, const std::string& kstr) {
		map_arr[key%N].mark_as_deleted(key, kstr);
	}

};

//unsigned long milliseconds_since_epoch =
//    std::chrono::system_clock::now().time_since_epoch() / 
//    std::chrono::milliseconds(1);

}

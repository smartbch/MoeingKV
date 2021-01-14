#pragma once
#include "internalkv.h"

namespace moeingkv {

inline uint64_t hashstr(const std::string& str, uint64_t seed) {
	return XXHash64::hash(str.data(), str.size(), seed);
}

struct metainfo {
	uint64_t seed;
	int64_t next_id;
	seeds get_seeds() {
		seeds res;
		for(int i=0; i<HASH_COUNT; i++) {
			res.u64[i] = hash(i, seed);
		}
		return res;
	}
};

class moeingkv {
	internalkv        ikv;
	metainfo          meta;
	friend class moeingkv_batch;
public:
	bool get(const std::string& key, std::string* value) {
		str_with_id data;
		bool ok = find(key, &data);
		if(ok) *value = data.str;
		return ok;
	}
	bool find(const std::string& key, str_with_id* out) {
		uint64_t hashkey = hashstr(key, meta.seed);
		return ikv.lookup(hashkey, key, out);
	}
};

class moeingkv_batch {
	moeingkv* parent;
	btree::btree_multimap<uint64_t, dstr_with_id> new_map;
public:
	void modify(const std::string& key, const std::string& value, bool is_del) {
		uint64_t hashkey = hashstr(key, parent->meta.seed);
		auto pos = new_map.end();
		for(auto iter = new_map.find(hashkey); iter != new_map.end(); iter++) {
			if(iter->second.dstr.first == key) {
				pos = iter;
				break;
			}
		}
		if(pos != new_map.end()) { //has a just-inserted entry
			if(is_del) {
				new_map.erase(pos);
			} else {
				pos->second.dstr.second = value;
			}
		} else {
			auto v = dstr_with_id{.id=1};
			v.dstr.first = key;
			v.dstr.second = key;
			if(is_del) v.id = -1;
			new_map.insert(std::make_pair(hashkey, v));
		}
	}
};

}

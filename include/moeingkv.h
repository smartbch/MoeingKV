#pragma once
#include "internalkv.h"

namespace moeingkv {

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
		return ikv.get(hashkey, key, out);
	}
};

class moeingkv_batch {
	moeingkv* parent;
	btree::btree_set<int64_t> del_list;
	btree::btree_map<std::string, str_with_id> new_map;
	int64_t next_id;
public:
	void put(const std::string& key, const std::string& value) {
		auto iter = new_map.find(key);
		if(iter != new_map.end()) {
			iter->second.str = value;
			return;
		}
		str_with_id data;
		bool found_it = parent->find(key, &data);
		if(found_it) {
			del_list.insert(data.id);
		}
		data.id = next_id++;
		data.str = value;
		new_map.insert(std::make_pair(key, data));
	}
	void erase(const std::string& key) {
		if(new_map.count(key) != 0) {
			new_map.erase(key);
			return;
		}
		str_with_id data;
		bool found_it = parent->find(key, &data);
		if(found_it) {
			del_list.insert(data.id);
		}
	}
};

}

#pragma once
#include "rawkv.h"

namespace chainkv {

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

struct dual_string {
	std::string first;
	std::string second;
	static dual_string from_one_str(const std::string& s) {
		dual_string ds;
		if(s.size() < 4) {
			return ds;
		}
		auto sz = size_t(uint8_t(s[3]));
		sz = (sz<<8) | size_t(uint8_t(s[2]));
		sz = (sz<<8) | size_t(uint8_t(s[1]));
		sz = (sz<<8) | size_t(uint8_t(s[0]));
		if(s.size() < 4 + sz) {
			return ds;
		}
		ds.first = s.substr(4, sz);
		ds.second = s.substr(4+sz, s.size() - 4 - sz);
		return ds;
	}
	std::string to_one_str() {
		std::string str;
		str.reserve(4 + first.size() + second.size());
		str.append(4, 0);
		auto sz = first.size();
		str[0] = char(sz);
		str[1] = char(sz>>8);
		str[2] = char(sz>>16);
		str[3] = char(sz>>24);
		str.append(first);
		str.append(second);
		return str;
	}
};

inline uint64_t hashstr(const std::string& str, uint64_t seed) {
	return XXHash64::hash(str.data(), str.size(), seed);
}

class chainkv {
	enum {
		RETRY_COUNT = 100,
	};
	rawkv rkv;
	metainfo meta;
	friend class chainkv_batch;
public:
	bool get(const std::string& key, std::string* value) {
		int64_t id;
		uint64_t hashkey;
		auto status = find(key, value, &id, &hashkey);
		return status != rawkv::NOT_FOUND;
	}
	rawkv::kv_status find(const std::string& key, std::string* value, int64_t* id, uint64_t* hashkey) {
		std::string one_str;
		for(int i=0; i<RETRY_COUNT; i++) {
			*hashkey = hashstr(key, meta.seed+i);
			auto status = rkv.get(*hashkey, &one_str, id);
			if(status == rawkv::NOT_FOUND) {
				return status;
			}
			auto dstr = dual_string::from_one_str(one_str);
			if(dstr.first == key) {
				*value = dstr.second;
				return status;
			}
		}
		return rawkv::NOT_FOUND;
	}
};

class chainkv_batch {
	chainkv* parent;
	std::vector<int64_t> del_ids;
	std::vector<uint64_t> del_hashkeys;
	btree::btree_map<uint64_t, dual_string> insert_entries;
public:
	void write() {
		for(auto iter = del_ids.begin(); iter != del_ids.end(); iter++) {
			parent->rkv.delete_id(*iter);
		}
		for(auto iter = del_hashkeys.begin(); iter != del_hashkeys.end(); iter++) {
			parent->rkv.fresh_erase(*iter);
		}
		for(auto iter = insert_entries.begin(); iter != insert_entries.end(); iter++) {
			parent->rkv.fresh_insert(iter->first, iter->second.to_one_str());
		}
	}
	void clear() {
		del_ids.clear();
		del_hashkeys.clear();
		insert_entries.clear();
	}
	void put(const dual_string& kv) {
		std::string old_value;
		int64_t old_id;
		uint64_t hashkey;
		auto status = parent->find(kv.first, &old_value, &old_id, &hashkey);
		if(status != rawkv::NOT_FOUND && kv.second == old_value) {
			return;
		}
		if(status == rawkv::IN_MEM) {
			insert_entries.insert(std::make_pair(old_id, kv));
		} else if(status == rawkv::ON_DISK) {
			del_ids.push_back(old_id);
			insert_entries.insert(std::make_pair(hashkey, kv));
		} else if(status == rawkv::NOT_FOUND) {
			insert_entries.insert(std::make_pair(hashkey, kv));
		} else {
			assert(false);
		}
	}
	void erase(const std::string& key) {
		std::string old_value;
		int64_t old_id;
		uint64_t hashkey;
		auto status = parent->find(key, &old_value, &old_id, &hashkey);
		if(status == rawkv::IN_MEM) {
			del_hashkeys.push_back(hashkey);
		} else if(status == rawkv::ON_DISK) {
			del_ids.push_back(old_id);
		} else if(status == rawkv::NOT_FOUND) {
			return;
		} else {
			assert(false);
		}
	}
};

}

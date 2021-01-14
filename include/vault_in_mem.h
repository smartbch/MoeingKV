#pragma once
#include "page.h"

namespace moeingkv {

// The temporary vault kept in memory. It has write-ahead log for persistency.
// KV pairs can be added to it, but cannot be deleted.
// According to the keys' highest bits, its internal storage is divided into several
// "rows" and each row has its own size.
// When you finish adding new pairs into it, its kv_producer can read out all the pairs in
// a specified row.
class vault_in_mem: public ds_with_log {
private:
	typedef btree::btree_multimap<uint64_t, dstr_with_id> i2str_map;
	i2str_map m[ROW_COUNT];

public:
	vault_in_mem(): m() {}
	vault_in_mem(const vault_in_mem& other) = delete;
	vault_in_mem& operator=(const vault_in_mem& other) = delete;
	vault_in_mem(vault_in_mem&& other) = delete;
	vault_in_mem& operator=(vault_in_mem&& other) = delete;

	size_t size_at_row(int row) {
		return m[row].size();
	}
	// Look up the corresponding 'str_with_id' for 'key_str'. 'key' must be short hash of 'key_str'
	// and its id must have not been marked as deleted in 'del_mark'. 
	// Returns whether a valid 'out' is found.
	bool lookup(uint64_t key, const std::string& key_str, str_with_id* out, bitarray* del_mark) {
		auto row = row_from_key(key);
		for(auto it = m[row].find(key); it != m[row].end() && it->second.dstr.kstr == key_str; it++) {
			if(!del_mark->get(it->second.id)) {
				out->str = it->second.dstr.vstr;
				out->id = it->second.id;
				return true;
			}
		}
		return false;
	}
	// write a log entry of adding kv
	void log_add_kv(uint64_t key, const dstr_with_id& value) {
		log_u64(key);
		log_i64(value.id);
		log_str(value.dstr.kstr);
		log_str(value.dstr.vstr);
	}
	// add new kv pair
	void add(uint64_t key, const dstr_with_id& value) {
		auto row = row_from_key(key);
		m[row].insert(std::make_pair(key, value));
	}
	bool load_data_from_log(const std::string& fname) {
		std::ifstream fin;
		fin.open(fname.c_str(), std::ios::in | std::ios::binary);
		if(!fin.is_open()) {
			std::cerr<<"Failed to open file "<<fname<<std::endl;
			return false;
		}
		while((fin.rdstate() & std::ios::eofbit) != 0) {
			uint64_t key;
			read_u64(fin, &key);
			dstr_with_id value;
			read_i64(fin, &value.id);
			read_str(fin, &value.dstr.kstr);
			read_str(fin, &value.dstr.vstr);
			add(key, value);
			if((fin.rdstate() & std::ios::failbit ) != 0) {
				std::cerr<<"Error when reading "<<fname<<std::endl;
				return false;
			}
		}
		return true;
	}
	class kv_prod: public kv_producer {
		i2str_map* m;
		i2str_map::iterator iter;
		bitarray* del_mark;
		friend class vault_in_mem;
	public:
		kv_pair peek() {
			return kv_pair{
				.id=iter->second.id,
				.key=iter->first,
				.value=iter->second.dstr
			};
		}
		kv_pair produce() {
			auto res = peek();
			for(; iter != m->end(); iter++) {
				if(!del_mark->get(iter->second.id)) break;
			}
			return res;
		}
		bool valid() {
			return iter != m->end();
		}
	};
	kv_prod get_kv_producer(int row, bitarray* del_mark) {
		kv_prod res;
		res.m=&m[row];
		res.iter=m[row].begin();
		res.del_mark=del_mark;
		return res;
	}
};

}

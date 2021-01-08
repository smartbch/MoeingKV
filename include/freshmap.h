#pragma once
#include "page.h"

namespace moeingkv {

class freshmap: public ds_with_log {
private:
	typedef btree::btree_multimap<uint64_t, dstr_with_id> i2str_map;
	i2str_map m[ROW_COUNT];

	void _log_add_kv(uint64_t key, const dstr_with_id& value) {
		log_u64(key);
		log_i64(value.id);
		log_str(value.dstr.first);
		log_str(value.dstr.second);
	}
	void _add(uint64_t key, const dstr_with_id& value) {
		auto row = row_from_key(key);
		m[row].insert(std::make_pair(key, value));
	}
public:
	freshmap(): m() {}
	freshmap(const freshmap& other) = delete;
	freshmap& operator=(const freshmap& other) = delete;
	freshmap(freshmap&& other) = delete;
	freshmap& operator=(freshmap&& other) = delete;

	size_t size_at_row(int row) {
		return m[row].size();
	}
	bool get(uint64_t key, const std::string& first_value, str_with_id* out, bitarray* del_mark) {
		auto row = row_from_key(key);
		auto iter = m[row].find(key);
		if(iter == m[row].end()) {
			return false;
		}
		for(; iter != m[row].end() && iter->second.dstr.first == first_value; iter++) {
			if(!del_mark->get(iter->second.id)) {
				out->str = iter->second.dstr.second;
				out->id = iter->second.id;
				return true;
			}
		}
		return false;
	}
	void add(uint64_t key, const dstr_with_id& value) {
		_add(key, value);
		_log_add_kv(key, value);
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
			auto row = row_from_key(key);
			dstr_with_id value;
			read_i64(fin, &value.id);
			read_str(fin, &value.dstr.first);
			read_str(fin, &value.dstr.second);
			_add(key, value);
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
		friend class freshmap;
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

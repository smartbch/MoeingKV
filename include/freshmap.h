#pragma once
#include "page.h"

namespace moeingkv {

class freshmap: public ds_with_log {
public:
	typedef btree::btree_multimap<uint64_t, dstr_with_id> i2str_map;
private:
	i2str_map m[ROW_COUNT];

public:
	size_t size_at_row(int row) {
		return m[row].size();
	}
	bool get(uint64_t key, const std::string& first_value, str_with_id* out, bitarray* del_mark) {
		auto row = row_from_key(key);
		auto iter = m[row].find(key);
		if(iter == m[row].end()) {
			return false;
		}
		for(; iter->second.dstr.first == first_value; iter++) {
			if(!del_mark->get(iter->second.id)) {
				out->str = iter->second.dstr.second;
				out->id = iter->second.id;
				return true;
			}
		}
		return false;
	}
	void add(uint64_t key, const dstr_with_id& value) {
		auto row = row_from_key(key);
		m[row].insert(std::make_pair(key, value));
		log_add_kv(key, value);
	}
	void log_add_kv(uint64_t key, const dstr_with_id& value) {
		log_u64(key);
		log_i64(value.id);
		log_str(value.dstr.first);
		log_str(value.dstr.second);
	}
	bool load_data_from_logs() {
		for(int i=0; i<file_list.size(); i++) {
			std::string fname = log_dir+"/"+std::to_string(file_list[i]);
			std::ifstream fin;
			fin.open(fname.c_str(), std::ios::in | std::ios::binary);
			if(!fin.is_open()) {
				std::cerr<<"Failed to open file "<<fname<<std::endl;
				return false;
			}
			while((fin.rdstate() & std::ios::eofbit ) != 0) {
				uint64_t key;
				read_u64(fin, &key);
				auto row = row_from_key(key);
				dstr_with_id value;
				read_i64(fin, &value.id);
				read_str(fin, &value.dstr.first);
				read_str(fin, &value.dstr.second);
				add(key, value);
				if((fin.rdstate() & std::ios::failbit ) != 0) {
					std::cerr<<"Error when reading "<<fname<<std::endl;
					return false;
				}
			}
		}
		return true;
	}
	class kv_prod: public kv_producer {
		i2str_map* m;
		i2str_map::iterator iter;
		bitarray* del_mark;
		uint64_t start;
		uint64_t end;
		friend class freshmap;
	public:
		uint64_t key_start() {
			return start;
		}
		uint64_t key_end() {
			return end;
		}
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
			if(start == ~uint64_t(0)) {
				start = res.key;
			}
			end = res.key;
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
		res.start = ~uint64_t(0);
		res.end = ~uint64_t(0);
		return res;
	}
};

}

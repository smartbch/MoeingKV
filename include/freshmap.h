#pragma once
#include "page.h"

namespace moeingkv {

class freshmap: public ds_with_log {
public:
	typedef btree::btree_multimap<uint64_t, dstr_with_id> i2str_map;
private:
	enum __const_t {
		CLEAR_ROW = 66,
		ADD_KV = 68,
	};

	i2str_map m[ROW_COUNT];

public:
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
	void insert(uint64_t key, const dstr_with_id& value) {
		auto row = row_from_key(key);
		m[row].insert(std::make_pair(key, value));
		log_set_kv(key, value);
	}
	void swap(int row, i2str_map& new_for_row) {
		m[row].swap(new_for_row);
		log_clear_row(row);
		for(auto iter = m[row].begin(); iter != m[row].end(); iter++) {
			log_set_kv(iter->first, iter->second);
		}
	}
	void log_clear_row(int row) {
		curr_log.put(char(CLEAR_ROW));
		log_u64(row_to_key(row));
	}
	void log_set_kv(uint64_t key, const dstr_with_id& value) {
		curr_log.put(char(ADD_KV));
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
				int cmd = fin.get();
				uint64_t key;
				read_u64(fin, &key);
				auto row = row_from_key(key);
				if(cmd == ADD_KV) {
					dstr_with_id value;
					read_i64(fin, &value.id);
					read_str(fin, &value.dstr.first);
					read_str(fin, &value.dstr.second);
					insert(key, value);
				} else if(cmd == CLEAR_ROW) {
					m[row].clear();
				} else {
					std::cerr<<"Unknown command: "<<cmd<<std::endl;
					return false;
				}
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

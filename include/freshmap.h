#pragma once
#include "page.h"

namespace moeingkv {

class freshmap: public ds_with_log {
public:
	typedef btree::btree_multimap<uint64_t, dstr_with_id> i2str_map;
private:
	enum __const_t {
		CLEAR_RANGE = 66,
		ADD_KV = 68,
	};

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
	void clear_range(uint64_t key_start, uint64_t key_end) {
		if(key_start == ~uint64_t(0)) {
			return; //do nothing for empty range
		}
		auto row = row_from_key(key_start);
		auto start = m[row].lower_bound(key_start);
		auto end = m[row].lower_bound(key_end);
		end++;
		m[row].erase(start, end);
		log_clear_range(key_start, key_end);
	}
	void log_range(uint64_t key_start, uint64_t key_end) {
		auto row = row_from_key(key_start);
		auto start = m[row].lower_bound(key_start);
		auto end = m[row].lower_bound(key_end);
		end++;
		log_clear_range(key_start, key_end);
		for(; start != end; start++) {
			log_add_kv(start->first, start->second);
		}
	}
	void log_clear_range(uint64_t key_start, uint64_t key_end) {
		curr_log.put(char(CLEAR_RANGE));
		log_u64(key_start);
		log_u64(key_end);
	}
	void log_add_kv(uint64_t key, const dstr_with_id& value) {
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
					add(key, value);
				} else if(cmd == CLEAR_RANGE) {
					uint64_t key_end;
					read_u64(fin, &key_end);
					clear_range(key, key_end);
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

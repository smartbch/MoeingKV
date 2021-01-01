#pragma once
#include <stdio.h>
#include <string.h>
#include <array>
#include <vector>
#include <fstream>
#include <unistd.h>
#include <dirent.h>
#include "common.h"
#include "cpp-btree-1.0.1/btree_map.h"

namespace moeingkv {

inline bool truncate_log(const std::string& log_dir, int num, off_t length) {
	std::string fname = log_dir+"/"+std::to_string(num);
	int res = truncate(fname.c_str(), length);
	if(res != 0) {
		std::cerr<<"Failed to truncate "<<fname<<std::endl;
	}
	return res==0;
}

inline bool get_log_nums(const std::string& log_dir, std::vector<int>* file_list) {
	DIR *dp = opendir(log_dir.c_str());
	if (dp == nullptr) {
		std::cerr<<"Cannot open dir: "<<log_dir<<std::endl;
		return false;
	}
	bool parse_ok = true;
	for(;;) {
		struct dirent *entry = readdir(dp);
		if(entry == nullptr) break;
		if(entry->d_type != DT_REG) continue;
		int i;
		int res = sscanf(entry->d_name, "%d", &i);
		if(res != 1) {
			std::cerr<<"Cannot parse file: "<<entry->d_name<<" i "<<i<<std::endl;
			parse_ok = false;
			continue;
		}
		file_list->push_back(i);
	}

	closedir(dp);
	sort(file_list->begin(), file_list->end());
	return parse_ok;
}

inline bool delete_useless_logs(const std::string& log_dir, const std::vector<int>& file_list, int lo, int hi) {
	for(int i=0; i<file_list.size(); i++) {
		if(lo <= file_list[i] && file_list[i] < hi ) continue;
		std::string fname = log_dir+"/"+std::to_string(file_list[i]);
		int res = remove(fname.c_str());
		if(res != 0) {
			std::cerr<<"Failed to delete "<<fname<<std::endl;
			return false;
		}
	}
	return true;
}

class ds_with_log {
protected:
	union int64_or_b8 {
		int64_t i64;
		char b8[8];
	};
	union uint64_or_b8 {
		uint64_t u64;
		char b8[8];
	};
	union uint32_or_b4 {
		uint32_t u32;
		char b4[4];
	};
	std::string log_dir;
	std::vector<int> file_list;
	std::ofstream curr_log;

	void log_u32(uint32_t i) {
		uint32_or_b4 data;
		data.u32 = i;
		curr_log.write(data.b4, 4);
	}
	void log_u64(uint64_t i) {
		uint64_or_b8 data;
		data.u64 = i;
		curr_log.write(data.b8, 8);
	}
	void log_i64(int64_t i) {
		int64_or_b8 data;
		data.i64 = i;
		curr_log.write(data.b8, 8);
	}
	void log_str(const std::string& s) {
		log_u32(s.size());
		curr_log.write(s.c_str(), s.size());
	}
	static std::ios::iostate read_u32(std::ifstream& fin, uint32_t* i) {
		uint32_or_b4 data;
		fin.read(data.b4, 4);
		*i = data.u32;
		return fin.rdstate();
	}
	static std::ios::iostate read_u64(std::ifstream& fin, uint64_t* i) {
		uint64_or_b8 data;
		fin.read(data.b8, 8);
		*i = data.u64;
		return fin.rdstate();
	}
	static std::ios::iostate read_i64(std::ifstream& fin, int64_t* i) {
		int64_or_b8 data;
		fin.read(data.b8, 8);
		*i = data.i64;
		return fin.rdstate();
	}
	static std::ios::iostate read_str(std::ifstream& fin, std::string* s) {
		uint32_t size;
		read_u32(fin, &size);
		std::vector<char> vec(size_t(size), char(0));
		fin.read(vec.data(), vec.size());
		*s = std::string(vec.data(), vec.size());
		return fin.rdstate();
	}
public:
	void init_logs(const std::string& dir, const std::vector<int>& list) {
		log_dir = dir;
		file_list = list;
	}
	size_t flush_log() {
		curr_log.flush();
		return curr_log.tellp();
	}
	bool open_log(int num) {
		std::string fname = log_dir+"/"+std::to_string(num);
		if(curr_log.is_open()) {
			std::cerr<<"The log file is already opened."<<std::endl;
			return false;
		}
		curr_log.open(fname.c_str(), std::ios::out | std::ios::app | std::ios::binary);
		if(!curr_log.is_open()) {
			std::cerr<<"Failed to open log file: "<<fname<<std::endl;
			return false;
		}
		return true;
	}
	bool switch_log(int num) {
		curr_log.close();
		std::string fname = log_dir+"/"+std::to_string(num);
		curr_log.open(fname.c_str(), std::ios::trunc | std::ios::app | std::ios::binary);
		if(!curr_log.is_open()) {
			std::cerr<<"Failed to open log file: "<<fname<<std::endl;
			return false;
		}
		return true;
	}
};

class bitarray: public ds_with_log {
	typedef std::array<uint8_t, BYTE_COUNT> arr_t;

	btree::btree_map<int64_t, arr_t*> tree;

	static arr_t* get_empty_arr() {//all bits are cleared by default
		auto a = new arr_t;
		memset(a->data(), 0, BYTE_COUNT);
		return a;
	}
	bool modify_and_return_old(int64_t pos, bool set, bool clear) {
		auto key = pos>>LEAF_BITS;
		pos = pos & LEAF_MASK;
		auto iter = tree.find(key);
		if(iter == tree.end()) {
			tree.insert(std::make_pair(key, get_empty_arr()));
			iter = tree.find(key);
		}
		auto arr_ptr = iter->second;
		auto byte_offset = pos/8;
		auto byte_mask = uint8_t(1) << (pos%8);
		auto old = (arr_ptr->at(byte_offset) & byte_mask) != 0;
		if(set) {
			arr_ptr->at(byte_offset) |= byte_mask;
			log_i64(pos);
		}
		if(clear) {
			arr_ptr->at(byte_offset) &= ~byte_mask;
			log_i64(-pos);
		}
		return old;
	}
public:
	bool get(int64_t pos) {
		return modify_and_return_old(pos, false, false);
	}
	bool set(int64_t pos) {
		return modify_and_return_old(pos, true, false);
	}
	bool clear(int64_t pos) {
		return modify_and_return_old(pos, false, true);
	}
	void prune_till(int64_t pos) {
		std::vector<int64_t> pos_list;
		auto iter = tree.upper_bound(pos>>LEAF_BITS);
		while(iter != tree.end()) {
			delete iter->second;
			pos_list.push_back(iter->first);
			iter--;
		}
		for(auto iter = pos_list.begin(); iter != pos_list.end(); iter++) {
			tree.erase(*iter);
		}
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
			int64_t i64;
			for(;;) {
				auto state = read_i64(fin, &i64);
				if((state & std::ios::eofbit ) != 0) {
					break;
				}
				if((state & std::ios::failbit ) != 0) {
					std::cerr<<"Failed to read file "<<fname<<std::endl;
					return false;
				}
				if(i64 > 0) { // positive for set
					set(i64);
				} else { //negative for clear
					clear(-i64);
				}
			}
		}
		return true;
	}
};

}

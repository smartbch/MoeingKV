#pragma once
#include <iostream>
#include <fstream>
#include <vector>
#include <unistd.h>
#include <dirent.h>
#include "common.h"

namespace moeingkv {

// truncate an existing log file, removing its useless ending part
inline bool truncate_log(const std::string& log_dir, int num, off_t length) {
	std::string fname = log_dir+"/"+std::to_string(num);
	int res = truncate(fname.c_str(), length);
	if(res != 0) {
		std::cerr<<"Failed to truncate "<<fname<<std::endl;
	}
	return res==0;
}

// convert the filenames under log_dir into integers.
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

// Consider the integers inside file_list as useless log files, if it is not in [lo, hi).
// Then delete these log files from log_dir.
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

// In-memory data structure with on-disk logs
class ds_with_log {
protected:
	std::string log_dir;
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
	void set_log_dir(const std::string& dir) {
		log_dir = dir;
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
	void close_log(int num) {
		curr_log.close();
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

}

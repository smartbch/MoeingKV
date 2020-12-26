#pragma once
#include <array>
#include <vector>
#include <string>
#include <string.h>
#include <unistd.h>
#include "bloomfilter256.h"
#include "bitarray.h"

namespace chainkv {

enum __page_size_t {
	PAGE_SIZE = 4096,
};

struct kv_pair {
	int64_t     id;
	uint64_t    key;
	std::string value;
	int size() {
		int res = 4/*offset*/ + 8/*key*/ + 8/*id*/ + value.size();
		assert(res + 8 <= PAGE_SIZE);
		return res;
	}
};

class page {
	std::array<uint8_t, PAGE_SIZE> arr;
	void write_u32(size_t offset, uint32_t v) {
		uint32_t* u32ptr = reinterpret_cast<uint32_t*>(arr.data()+offset);
		*u32ptr = v;
	}
	uint32_t read_u32(size_t offset) {
		uint32_t* u32ptr = reinterpret_cast<uint32_t*>(arr.data()+offset);
		return *u32ptr;
	}
	void write_i64(size_t offset, int64_t v) {
		int64_t* i64ptr = reinterpret_cast<int64_t*>(arr.data()+offset);
		*i64ptr = v;
	}
	int64_t read_i64(size_t offset) {
		int64_t* i64ptr = reinterpret_cast<int64_t*>(arr.data()+offset);
		return *i64ptr;
	}
	void write_u64(size_t offset, uint64_t v) {
		uint64_t* u64ptr = reinterpret_cast<uint64_t*>(arr.data()+offset);
		*u64ptr = v;
	}
	uint64_t read_u64(size_t offset) {
		uint64_t* u64ptr = reinterpret_cast<uint64_t*>(arr.data()+offset);
		return *u64ptr;
	}
public:
	uint8_t* data() {
		return arr.data();
	}
	void fill_with(const std::vector<kv_pair>& in_list) {
		size_t start = 0;
		uint32_t value_pos = 4/*count*/ + 4/*ending*/ + 20 * in_list.size();
		write_u32(start, uint32_t(in_list.size()));
		for(auto iter=in_list.begin(); iter != in_list.end(); iter++) {
			write_u32(start, value_pos);
			value_pos += iter->value.size();
			start += 4;
			write_u64(start, iter->key);
			start += 8;
			write_i64(start, iter->id);
			start += 8;
		}
		write_u32(start, value_pos);
		start += 4;
		for(auto iter=in_list.begin(); iter != in_list.end(); iter++) {
			memcpy(arr.data(), iter->value.data(), iter->value.size());
			start += iter->value.size();
		}
	}
	bool read_at(int i, uint64_t* key, kv_pair* kv) {
		auto key_pos = 4 + i*20 + 4;
		kv->key = read_u64(key_pos);
		if(key != nullptr && *key != kv->key) {
			return false;
		}
		int offset = read_u32(4 + i*20);
		int next_offset = read_u32(4 + (i+1)*20);
		kv->value = std::string(reinterpret_cast<const char*>(data() + offset), next_offset - offset);
		kv->id = read_i64(4 + i*20 + 4 + 8);
		return true;
	}
	kv_pair lookup(uint64_t key) {
		kv_pair kv{.id=-1};
		int count = read_u32(0);
		for(int i=0; i<count; i++) {
			if(read_at(i, &key, &kv)) break;
		}
		return kv;
	}
	void extract_to(std::vector<kv_pair>* vec, bitarray* del_mark) {
		vec->clear();
		kv_pair kv{.id=-1};
		int count = read_u32(0);
		for(int i=0; i<count; i++) {
			read_at(i, nullptr, &kv);
			if(!del_mark->get(kv.id)) {
				vec->push_back(kv);
			}
		}
	}
};

class kv_producer {
public:
	virtual kv_pair peek() = 0;
	virtual kv_pair produce() = 0;
	virtual bool has_more() = 0;
};

class kv_consumer {
public:
	virtual void consume(kv_pair kv) = 0;
	virtual void flush() = 0;
};

// ============================

class merged_kv_producer {
	kv_producer* _a;
	kv_producer* _b;
public:
	merged_kv_producer(kv_producer* a, kv_producer* b): _a(a), _b(b) {}
	kv_pair peek() {
		if(!_a->has_more()) {
			return _b->peek();
		}
		if(!_b->has_more()) {
			return _a->peek();
		}
		if(_a->peek().key < _b->peek().key) {
			return _a->peek();
		}
		return _b->peek();
	};
	kv_pair produce() {
		if(!_a->has_more()) {
			return _b->produce();
		}
		if(!_b->has_more()) {
			return _a->produce();
		}
		if(_a->peek().key < _b->peek().key) {
			return _a->produce();
		}
		return _b->produce();
	};
	bool has_more() {
		return _a->has_more() || _b->has_more();
	}
};


class kv_reader : public kv_producer {
	int fd;
	size_t offset;
	size_t end_offset;
	std::vector<kv_pair> pairs;
	int pair_idx;
	bitarray* del_mark;
	void load_page() {
		page pg;
		auto sz = pread(fd, pg.data(), PAGE_SIZE, offset);
		assert(sz == PAGE_SIZE);
		offset += PAGE_SIZE;
		pg.extract_to(&pairs, del_mark);
		pair_idx = 0;
	}
public:
	kv_reader(size_t start, size_t end, int fd, bitarray* del_mark):
	fd(fd), offset(start), end_offset(end), del_mark(del_mark) {
		pairs.reserve(100);
		load_page();
	}
	kv_pair peek() {
		return pairs[pair_idx];
	}
	kv_pair produce() {
		if(!has_more()) return kv_pair{};
		auto kv = peek();
		if(pair_idx < pairs.size()) {
			pair_idx++;
		} else {
			load_page();
		}
		return kv;
	}
	bool has_more() {
		return offset <= end_offset || pair_idx < pairs.size();
	}
};

class kv_packer : public kv_consumer {
	int fd;
	std::vector<kv_pair> kv_list;
	int used_size;
	bloomfilter256* bf256;
	uint8_t col;
public:
	kv_packer(int fd, bloomfilter256* bf, uint8_t c): fd(fd), used_size(8), bf256(bf), col(c) {
		kv_list.reserve(100);
	}
	void consume(kv_pair kv) {
		bf256->add_at(col, kv.key);
		used_size += kv.size();
		if(used_size > PAGE_SIZE) {
			flush();
		}
		kv_list.push_back(kv);
	}
	void flush() {
		if(kv_list.size() == 0) return;
		page pg;
		pg.fill_with(kv_list);
		auto sz = write(fd, pg.data(), PAGE_SIZE);
		assert(sz == PAGE_SIZE);
		kv_list.clear();
		used_size = 8;
	}
};

}

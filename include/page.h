#pragma once
#include <array>
#include <vector>
#include <string>
#include <string.h>
#include <unistd.h>
#include <algorithm>
#include "bloomfilter.h"
#include "bitarray.h"
#include "u64vec.h"

namespace moeingkv {

enum __page_size_t {
	PAGE_SIZE = 4096,
	PAGE_INIT_SIZE = 8,
};

struct kv_pair {
	int64_t     id;
	uint64_t    key;
	dual_string value;
	size_t size() const {
		return 2/*offset*/ + 8/*id*/ + 8/*key*/ + 4/*two lengths*/ + value.size();
	}
};

class page {
	std::array<char, PAGE_SIZE> arr;
	void write_u16(size_t offset, uint16_t v) {
		uint16_t* u16ptr = reinterpret_cast<uint16_t*>(arr.data()+offset);
		*u16ptr = v;
	}
	uint16_t read_u16(size_t offset) {
		uint16_t* u16ptr = reinterpret_cast<uint16_t*>(arr.data()+offset);
		return *u16ptr;
	}
	void write_i64(size_t offset, int64_t v) {
		int64_t* i64ptr = reinterpret_cast<int64_t*>(arr.data()+offset);
		*i64ptr = v;
	}
	void write_str(size_t offset, const std::string& s) {
		memcpy(arr.data()+offset, s.data(), s.size());
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
	void read_str(size_t offset, std::string* s, size_t size) {
		*s = std::string(arr.data()+offset, size);
	}
public:
	page(): arr() {}
	page(const page& other) = delete;
	page& operator=(const page& other) = delete;
	page(page&& other) = delete;
	page& operator=(page&& other) = delete;

	char* data() {
		return arr.data();
	}
	void fill_with(const std::vector<kv_pair>& in_list) {
		size_t start = PAGE_INIT_SIZE;
		for(auto iter=in_list.begin(); iter != in_list.end(); iter++) {
			write_u64(start, iter->key); start += 8;
		}
		uint16_t offset = start + 2 * in_list.size();
		for(auto iter=in_list.begin(); iter != in_list.end(); iter++) {
			write_u16(start, offset); start+=2;
			offset += 8/*id*/ + 4/*two lengths*/ + iter->value.size();
		}
		for(auto iter=in_list.begin(); iter != in_list.end(); iter++) {
			write_i64(start, iter->id); start += 8;
			write_u16(start, uint16_t(iter->value.first.size())); start += 2;
			write_u16(start, uint16_t(iter->value.second.size())); start += 2;
			write_str(start, iter->value.first); start += iter->value.first.size();
			write_str(start, iter->value.second); start += iter->value.second.size();
		}
	}
	bool lookup(uint64_t key, const std::string& first_value, str_with_id* out, bitarray* del_mark) {
		size_t count = read_u16(0);
		uint64_t* keyptr_start = reinterpret_cast<uint64_t*>(arr.data()+PAGE_INIT_SIZE);
		uint64_t* keyptr_end = keyptr_start + count;
		uint64_t* keyptr = std::lower_bound(keyptr_start, keyptr_end, key);
		for(; *keyptr == key && keyptr != keyptr_end; keyptr++) {
			size_t idx = keyptr - keyptr_start;
			size_t offset = PAGE_INIT_SIZE + 8 * count + 2 * idx;
			size_t pos = read_u16(offset);
			size_t first_value_len = read_u16(pos + 8);
			char* first_value_start = arr.data() + pos + 12;
			if(first_value_len != first_value.size() ||
			   memcmp(first_value_start, first_value.data(), first_value_len) != 0) {
				continue;
			}
			auto id = read_i64(pos);
			if(!del_mark->get(id)) { //not deleted
				char* second_value_start = first_value_start + first_value_len;
				size_t second_value_len = read_u16(pos + 10);
				out->str = std::string(second_value_start, second_value_len);
				out->id = id;
				return true;
			}
		}
		return false;
	}
	void extract_to(std::vector<kv_pair>* vec, bitarray* del_mark) {
		vec->clear();
		size_t count = read_u16(0);
		uint64_t* keyptr_start = reinterpret_cast<uint64_t*>(arr.data()+PAGE_INIT_SIZE);
		for(size_t idx = 0; idx < count; idx++) {
			size_t offset = PAGE_INIT_SIZE + 8 * count + 2 * idx;
			size_t pos = read_u16(offset);
			kv_pair kv;
			kv.id = read_i64(pos);
			if(del_mark->get(kv.id)) {
				continue;
			}
			char* first_value_start = arr.data() + pos + 12;
			size_t first_value_len = read_u16(pos + 8);
			char* second_value_start = arr.data() + pos + 12 + first_value_len;
			size_t second_value_len = read_u16(pos + 10);
			kv.key = keyptr_start[idx];
			kv.value.first = std::string(first_value_start, first_value_len);
			kv.value.second = std::string(second_value_start, second_value_len);
			vec->push_back(kv);
		}
	}
};

class kv_producer {
public:
	virtual kv_pair peek() = 0;
	virtual kv_pair produce() = 0;
	virtual bool valid() = 0;
};

// ============================

class merged_kv_producer {
	kv_producer* _a;
	kv_producer* _b;
	uint64_t last_key;
	bool never_produced;
	kv_pair _produce() {
		if(!_a->valid()) {
			return _b->produce();
		}
		if(!_b->valid()) {
			return _a->produce();
		}
		if(_a->peek().key < _b->peek().key) {
			return _a->produce();
		}
		return _b->produce();
	};
public:
	merged_kv_producer(kv_producer* a, kv_producer* b): _a(a), _b(b), never_produced(true) {}
	bool in_middle_of_same_key() {
		if(never_produced) return false;
		return last_key == peek().key && valid();
	}
	kv_pair peek() {
		if(!_a->valid()) {
			return _b->peek();
		}
		if(!_b->valid()) {
			return _a->peek();
		}
		if(_a->peek().key < _b->peek().key) {
			return _a->peek();
		}
		return _b->peek();
	};
	kv_pair produce() {
		never_produced = false;
		auto res = _produce();
		last_key = res.key;
		return res;
	}
	bool valid() {
		return _a->valid() || _b->valid();
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
		if(!valid()) return kv_pair{};
		auto kv = peek();
		if(pair_idx < pairs.size()) {
			pair_idx++;
		} else if(offset < end_offset) {
			load_page();
		}
		return kv;
	}
	bool valid() {
		return offset < end_offset || (offset == end_offset && pair_idx < pairs.size());
	}
};

class kv_packer {
	int                  fd;
	std::vector<kv_pair> kv_list;
	int                  used_size;
	bloomfilter*         bf;
	u64vec*              vec;
public:
	kv_packer(int fd, bloomfilter* bf, u64vec* v):
		fd(fd), used_size(PAGE_INIT_SIZE), bf(bf), vec(v) {
		kv_list.reserve(100);
	}
	void consume(kv_pair kv) {
		bf->add(kv.key);
		used_size += kv.size();
		kv_list.push_back(kv);
	}
	bool can_consume(kv_pair kv) {
		return used_size + kv.size() < PAGE_SIZE;
	}
	void flush() {
		if(kv_list.size() == 0) return;
		vec->append(kv_list[0].key);
		page pg;
		pg.fill_with(kv_list);
		auto sz = write(fd, pg.data(), PAGE_SIZE);
		assert(sz == PAGE_SIZE);
		kv_list.clear();
		used_size = PAGE_INIT_SIZE;
	}
};

}

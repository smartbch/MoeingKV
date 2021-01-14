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

// Raw memory of PAGE_SIZE bytes, which can be loaded from or stored to SSD directly. 
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
	// fill the raw bytes with content in 'in_list'
	void fill_with(const std::vector<kv_pair>& in_list) {
		size_t start = PAGE_INIT_SIZE;
		for(auto iter=in_list.begin(); iter != in_list.end(); iter++) {
			write_u64(start, iter->key); start += 8;
		}
		uint16_t offset = start + 2 * in_list.size();
		for(auto iter=in_list.begin(); iter != in_list.end(); iter++) {
			write_u16(start, offset); start+=2;
			offset += 8/*id*/ + 4/*two lengths*/ +
				iter->value.kstr.size() + iter->value.vstr.size();
		}
		for(auto iter=in_list.begin(); iter != in_list.end(); iter++) {
			write_i64(start, iter->id); start += 8;
			write_u16(start, uint16_t(iter->value.kstr.size())); start += 2;
			write_u16(start, uint16_t(iter->value.vstr.size())); start += 2;
			write_str(start, iter->value.kstr); start += iter->value.kstr.size();
			write_str(start, iter->value.vstr); start += iter->value.vstr.size();
		}
	}
	// Look up the corresponding 'str_with_id' for 'key_str'. 'key' must be short hash of 'key_str'
	// and its id must have not been marked as deleted in 'del_mark'. 
	// Returns whether a valid 'out' is found.
	bool lookup(uint64_t key, const std::string& key_str, str_with_id* out, bitarray* del_mark) {
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
			if(first_value_len != key_str.size() ||
			   memcmp(first_value_start, key_str.data(), first_value_len) != 0) {
				continue;
			}
			// Checking del_mark is time-consuming, so we only check it when key_str matches.
			auto id = read_i64(pos);
			if(!del_mark->get(id)) {
				char* second_value_start = first_value_start + first_value_len;
				size_t second_value_len = read_u16(pos + 10);
				out->str = std::string(second_value_start, second_value_len);
				out->id = id;
				return true;
			}
		}
		return false;
	}
	// Extract the kv pairs stored in current page out, except the ones marked as deleted in 'del_mark'
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
			size_t first_value_len = read_u16(pos + 8);
			size_t second_value_len = read_u16(pos + 10);
			char* first_value_start = arr.data() + pos + 12;
			char* second_value_start = first_value_start + first_value_len;
			kv.key = keyptr_start[idx];
			kv.value.kstr = std::string(first_value_start, first_value_len);
			kv.value.vstr = std::string(second_value_start, second_value_len);
			vec->push_back(kv);
		}
	}
};

// It produces a stream of sorted kv_pairs (keys from small to large)
class kv_producer {
public:
	virtual kv_pair peek() = 0;
	virtual kv_pair produce() = 0;
	virtual bool valid() = 0;
};

// ============================

// It merges kv_pair streams from two kv_producer into one stream, while keeping keys'
// increasing order.
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

// It reads kv_pairs from the pages in a vault
class kv_reader : public kv_producer {
	int fd; // file descriptor of the vault
	size_t offset; // start position for reading
	size_t end_offset; // end position for reading
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

// It packs a kv_pair stream into pages and store them to vault file
// The first keys of these pages are recorded in 'vec'
class kv_packer {
	int                  fd;
	std::vector<kv_pair> kv_list; // a cache for pending kv_pair
	int                  used_size;
	bloomfilter*         bf;
	u64vec*              vec;
public:
	kv_packer(int fd, bloomfilter* bf, u64vec* v):
		fd(fd), used_size(PAGE_INIT_SIZE), bf(bf), vec(v) {
		kv_list.reserve(100);
	}
	size_t size_of_kv_pair(const kv_pair& kv) {
		return 2/*offset*/ + 8/*id*/ + 8/*key*/ + 4/*two lengths*/ +
			kv.value.kstr.size() + kv.value.vstr.size();
	}
	// consume a kv_pair and store it in cache
	void consume(const kv_pair& kv) {
		bf->add(kv.key);
		used_size += size_of_kv_pair(kv);
		kv_list.push_back(kv);
	}
	// Returns whether current page can consume 'kv', if not, you must run 'flush' first.
	bool can_consume(const kv_pair& kv) {
		return used_size + size_of_kv_pair(kv) < PAGE_SIZE;
	}
	// flush the cache into disk
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

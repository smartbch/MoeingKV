#pragma once
#include <type_traits>
#include <atomic>
#include <vector>
#include <memory>
#include <sys/mman.h>
#include <string.h>
#include "xxhash64.h"
#include "common.h"

namespace moeingkv {

// a bit vector of 256 bits
class bits256 {
	std::atomic_ullong d[4];
public:

	bits256& operator|=(const bits256& other) {
		for(int i=0; i<4; i++) {
			d[i].fetch_or(other.d[i].load());
		}
		return *this;
	}
	bool get(int vault_lsb) {
		selector64 sel(vault_lsb);
		return (d[sel.n].load() & sel.mask) != 0;
	}
	void clear(int vault_lsb) {
		selector64 sel(vault_lsb);
		d[sel.n] &= ~sel.mask;
	}
	void set(int vault_lsb) {
		selector64 sel(vault_lsb);
		d[sel.n] |= sel.mask;
	}
};

inline uint64_t hash(uint64_t key, uint64_t seed) {
	uint64_or_b8 data;
	data.u64 = key;
	return XXHash64::hash(data.b8, 8, seed);
}

// one bloomfilter
class bloomfilter {
	size_t                _size;
	std::vector<uint64_t> _data;
	seeds*                _seeds;
public:
	bloomfilter(size_t size, seeds* s): _size(64*((size+63)/64)), _data(_size, 0), _seeds(s) {}
	size_t size() const {
		return _size;
	}
	void add(uint64_t key) {
		for(int i=0; i<HASH_COUNT; i++) {
			uint64_t h = hash(key, _seeds->u64[i]);
			uint64_t pos = h % _size;
			_data[pos/64] |= uint64_t(1)<<(pos%64);
		}
	}
	bool get_bit(size_t pos) const {
		assert(pos < _size);
		return (_data[pos/64] & (uint64_t(1)<<(pos%64))) != 0;
	}
};

// 256 bloomfilters of the same size. It has the same functionality of 256 bloomfilters while its
// cache locality is much better.
class bloomfilter256 {
	size_t   _size;
	bits256* _data;
	seeds*   _seeds;
	int init_data() {
		auto num_bytes = _size*sizeof(bits256);
		_data = (bits256 *) mmap (nullptr, num_bytes, PROT_READ|PROT_WRITE,
		       MAP_PRIVATE|MAP_ANONYMOUS, -1, 0);
		return num_bytes;
	}
	bloomfilter256(const bloomfilter256* other, int factor): _size(other->_size*factor), _seeds(other->_seeds) {
		auto num_bytes = init_data();
		auto unit = num_bytes/factor;
		for(int i=0; i<factor; i++) {
			memcpy(((char*)_data)+i*unit, (char*)other->_data, unit);
		}
	}
public:
	bloomfilter256* double_sized() const {
		return new bloomfilter256(this, 2);
	}
	bloomfilter256(size_t size, seeds* s): _size(size), _seeds(s) {
		auto num_bytes = init_data();
		memset((char*)_data, 0, num_bytes);
	}
	bloomfilter256(): _size(0), _data(nullptr), _seeds(nullptr) {}
	~bloomfilter256() {
		if(_data != nullptr) {
			munmap((void*)_data, _size*sizeof(bits256));
		}
	}

	bloomfilter256(const bloomfilter256& other) = delete;
	bloomfilter256& operator=(const bloomfilter256& other) = delete;

	bloomfilter256(bloomfilter256&& other) {
		this->_size = other._size;
		this->_data = other._data;
		this->_seeds = other._seeds;
		other._size = 0;
		other._data = nullptr;
		other._seeds = nullptr;
	}
	bloomfilter256& operator=(bloomfilter256&& other) {
		this->_size = other._size;
		this->_data = other._data;
		this->_seeds = other._seeds;
		other._size = 0;
		other._data = nullptr;
		other._seeds = nullptr;
		return *this;
	}

	size_t size() const {
		return _size;
	}
	// assign bf's value to the bloomfilter at the position of 'vault_lsb'
	void assign_at(uint8_t vault_lsb, const bloomfilter* bf) {
		assert(bf->size() == _size);
		for(int i=0; i<_size; i++) {
			if(bf->get_bit(i)) { // set
				_data[i].set(vault_lsb);
			} else { // clear
				_data[i].clear(vault_lsb);
			}
		}
	}
	// clear the bloomfilter at the position of 'vault_lsb'
	void clear_at(uint8_t vault_lsb) {
		for(int i=0; i<_size; i++) {
			_data[i].clear(vault_lsb);
		}
	}
	void add_at(uint8_t vault_lsb, uint64_t key) {
		for(int i=0; i<HASH_COUNT; i++) {
			uint64_t h = hash(key, _seeds->u64[i]);
			_data[h%_size].set(vault_lsb);
		}
	}
	void get_mask(uint64_t key, bits256& res) const {
		uint64_t h[HASH_COUNT];
		for(int i=0; i<HASH_COUNT; i++) {
			h[i] = hash(key, _seeds->u64[i]) % _size;
			__builtin_prefetch(_data+h[i], 0, 0);
		}
		for(int i=0; i<HASH_COUNT; i++) {
			res |= _data[h[i]];
		}
	}
};

}

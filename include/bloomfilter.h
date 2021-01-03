#pragma once
#include <atomic>
#include <sys/mman.h>
#include <string.h>
#include "xxhash64.h"

namespace moeingkv {

enum {
	HASH_COUNT = 8,
};

struct seeds {
	uint64_t u64[HASH_COUNT];
};

union bits64 {
	uint8_t  u8[8];
	uint64_t u64;
};

struct selector {
	int n;
	uint64_t mask;
	selector(uint8_t vault_lsb) {
		n = vault_lsb>>6;
		mask = uint64_t(1) << uint64_t(vault_lsb%64);
	}
};

class bits256 {
	std::atomic_ullong d[4];
public:
	bits256& operator|=(const bits256& other) {
		for(int i=0; i<4; i++) d[i] |= other.d[i];
		return *this;
	}
	bool get(int vault_lsb) {
		selector sel(vault_lsb);
		return (d[sel.n].load() & sel.mask) != 0;
	}
	void clear(int vault_lsb) {
		selector sel(vault_lsb);
		d[sel.n] &= ~sel.mask;
	}
	void set(int vault_lsb) {
		selector sel(vault_lsb);
		d[sel.n] |= sel.mask;
	}
};


inline uint64_t hash(uint64_t key, uint64_t seed) {
	bits64 b64;
	b64.u64 = key;
	return XXHash64::hash(b64.u8, 8, seed);
}

//DEL//class bloomfilter {
//DEL//	size_t   _size;
//DEL//	std::vector<uint64_t> _data;
//DEL//	seeds*   _seeds;
//DEL//public:
//DEL//	bloomfilter(size_t size, seeds* s): _size(64*((size+63)/64)), _data(_size, 0), _seeds(s) {}
//DEL//	size_t size() {
//DEL//		return _size;
//DEL//	}
//DEL//	void add(uint64_t key) {
//DEL//		for(int i=0; i<HASH_COUNT; i++) {
//DEL//			uint64_t h = hash(key, _seeds->u64[i]);
//DEL//			uint64_t pos = h % _size;
//DEL//			_data[pos/64] |= uint64_t(1)<<(pos%64);
//DEL//		}
//DEL//	}
//DEL//	bool get_bit(size_t pos) const {
//DEL//		assert(pos < _size);
//DEL//		return (_data[pos/64] & (uint64_t(1)<<(pos%64))) != 0;
//DEL//	}
//DEL//};

class bloomfilter256 {
	size_t   _size;
	bits256* _data;
	seeds*   _seeds;
	bloomfilter256(const bloomfilter256* other): _size(other->_size*2), _seeds(other->_seeds) {
		auto num_bytes = _size*sizeof(bits256);
		_data = (bits256 *) mmap (nullptr, num_bytes, PROT_READ|PROT_WRITE,
		       MAP_PRIVATE|MAP_ANONYMOUS, -1, 0);
		memcpy((char*)_data, (char*)other->_data, num_bytes/2);
		memcpy(((char*)_data)+num_bytes/2, (char*)other->_data, num_bytes/2);
	}
public:
	bloomfilter256* double_sized() {
		return new bloomfilter256(this);
	}
	bloomfilter256(size_t size, seeds* s): _size(size), _seeds(s) {
		auto num_bytes = _size*sizeof(bits256);
		_data = (bits256 *) mmap (nullptr, num_bytes, PROT_READ|PROT_WRITE,
		       MAP_PRIVATE|MAP_ANONYMOUS, -1, 0);
		memset((char*)_data, 0, num_bytes);
	}
	~bloomfilter256() {
		munmap((void*)_data, _size*sizeof(bits256));
	}
	size_t size() {
		return _size;
	}
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
	void get_mask(uint64_t key, bits256& res) {
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

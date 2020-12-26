#pragma once
#include <atomic>
#include <sys/mman.h>
#include <string.h>
#include "xxhash64.h"
#include "bloomfilter256.h"

namespace chainkv {

enum {
	HASH_COUNT = 8,
};

struct seeds {
	uint64_t u64[HASH_COUNT];
};

struct pos_list {
	uint8_t data[256];
	size_t size;
};

struct bits256 {
	std::atomic_ullong d[4];
};

union bits64 {
	uint8_t  u8[8];
	uint64_t u64;
};

struct selector {
	int n;
	uint64_t mask;
	selector(uint8_t col) {
		n = col>>6;
		mask = uint64_t(1) << uint64_t(col%64);
	}
};

inline uint64_t hash(uint64_t key, uint64_t seed) {
	bits64 b64;
	b64.u64 = key;
	return XXHash64::hash(b64.u8, 8, seed);
}

class bloomfilter256 {
	size_t   _count;
	bits256* _data;
	seeds*   _seeds;
public:
	bloomfilter256(size_t count, seeds* s): _count(count), _seeds(s) {
		auto num_bytes = _count*sizeof(bits256);
		_data = (bits256 *) mmap (nullptr, num_bytes, PROT_READ|PROT_WRITE,
		       MAP_PRIVATE|MAP_ANONYMOUS, -1, 0);
		memset((char*)_data, 0, num_bytes);
	}
	~bloomfilter256() {
		munmap((void*)_data, _count*sizeof(bits256));
	}
	void clear_at(uint8_t col) {//TODO use me
		selector sel(col);
		for(int i=0; i<_count; i++) {
			_data[i].d[sel.n] &= ~sel.mask;
		}
	}
	void add_at(uint8_t col, uint64_t key) {
		selector sel(col);
		for(int i=0; i<HASH_COUNT; i++) {
			uint64_t h = hash(key, _seeds->u64[i]);
			_data[h%_count].d[sel.n] |= sel.mask;
		}
	}
	pos_list get_pos_list(uint64_t key, uint8_t gap_col) {
		uint64_t u64[4];
		for(int k=0; k<4; k++) u64[k]=0;
		uint64_t h[HASH_COUNT];
		for(int i=0; i<HASH_COUNT; i++) {
			h[i] = hash(key, _seeds->u64[i]) % _count;
			__builtin_prefetch(_data+h[i], 0, 0);
		}
		for(int i=0; i<HASH_COUNT; i++) {
			auto idx = h[i];
			for(int k=0; k<4; k++) {
				u64[k] |= _data[idx].d[k].load();
			}
		}
		auto l = pos_list{.size=0};
		for(int i=int(gap_col)+255; i >= int(gap_col)+1; i--) {
			int ii = i % 256;
			int k = i / 64;
			int j = i % 64;
			auto mask = uint64_t(1) << j;
			if((u64[k] & mask) != 0) {
				l.data[l.size] = ii;
				l.size++;
			}
		}
		return l;
	}
};

}

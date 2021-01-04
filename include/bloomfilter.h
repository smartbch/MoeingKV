#pragma once
#include <type_traits>
#include <atomic>
#include <vector>
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

#define TO_DEL_THIS (uint64_t(1)<<63)

class releasable {
	std::atomic_ullong status;
public:
	bool request_to_release() {
		auto old_value = std::atomic_fetch_or(&status, TO_DEL_THIS);
		return old_value == 0;
	}
	void begin_read() {
		std::atomic_fetch_add(&status, uint64_t(1));
	}
	bool end_read_and_check_release_request() {
		auto old_value = std::atomic_fetch_sub(&status, uint64_t(1));
		return old_value == (TO_DEL_THIS + 1);
	}
};

template<typename T, int N>
class pointer_array {
	static_assert(std::is_base_of<releasable, T>::value, "only support releasable objects");
	std::atomic<T*> arr[N];
	void try_delete(T* ptr) {
		if(ptr != nullptr) {
			bool ok = ptr->request_to_release();
			if(ok) delete ptr;
		}
	}
public:
	~pointer_array() {
		for(int i=0; i<N; i++) {
			T* ptr = arr[i].load();
			try_delete(ptr);
		}
	}
	void replace(int i, T* new_ptr) {
		T* ptr = std::atomic_exchange(arr + i, new_ptr);
		try_delete(ptr);
	}
	T* get_ptr(int i) {
		auto ptr = arr[i].load();
		ptr->begin_read();
		return ptr;
	}
	void return_ptr(T* ptr) {
		bool need_to_release = ptr->end_read_and_check_release_request();
		if(need_to_release) delete ptr;
	}
};

class bloomfilter {
	size_t   _size;
	std::vector<uint64_t> _data;
	seeds*   _seeds;
public:
	bloomfilter(size_t size, seeds* s): _size(64*((size+63)/64)), _data(_size, 0), _seeds(s) {}
	size_t size() {
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

class bloomfilter256: public releasable {
	size_t   _size;
	bits256* _data;
	seeds*   _seeds;
	bloomfilter256(const bloomfilter256* other): releasable(), _size(other->_size*2), _seeds(other->_seeds) {
		auto num_bytes = _size*sizeof(bits256);
		_data = (bits256 *) mmap (nullptr, num_bytes, PROT_READ|PROT_WRITE,
		       MAP_PRIVATE|MAP_ANONYMOUS, -1, 0);
		memcpy((char*)_data, (char*)other->_data, num_bytes/2);
		memcpy(((char*)_data)+num_bytes/2, (char*)other->_data, num_bytes/2);
	}
public:
	bloomfilter256* double_sized() const {
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
	size_t size() const {
		return _size;
	}
	void assign_at(uint8_t vault_lsb, const bloomfilter* bf) {
		for(int i=0; i<_size; i++) {
			if(bf->get_bit(i)) { // set
				_data[i].set(vault_lsb);
			} else { // clear
				_data[i].clear(vault_lsb);
			}
		}
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

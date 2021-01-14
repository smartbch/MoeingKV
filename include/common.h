#pragma once
#include <string>

namespace moeingkv {

enum __const_t {
	VAULT_COUNT = 256,

	ROW_COUNT = 256,
	ROW_BITS = 8,

	HASH_COUNT = 8,
	BITS_PER_ENTRY = 20,

	CACHE_SHARD_COUNT = 1024,
};

#define MEM_VAULT_LOG_DIR ("mvault")
#define DISK_VAULT_DIR ("vault")
#define DEL_LOG_DIR ("del")
#define META_FILE ("meta.txt")

// select a bit in u64 vector&array
struct selector64 {
	int n;
	uint64_t mask;
	selector64(int i) {
		n = i/64;
		mask = uint64_t(1) << uint64_t(i%64);
	}
};

// hash seeds for bloomfilter
struct seeds {
	uint64_t u64[HASH_COUNT];
	seeds(const seeds& other) {
		for(int i=0; i<HASH_COUNT; i++) u64[i]=other.u64[i];
	}
	seeds() {
		for(int i=0; i<HASH_COUNT; i++) u64[i]=0;
	}
};

// extract row id from key
inline int row_from_key(uint64_t key) {
	return int(key>>(64-ROW_BITS)) % ROW_COUNT;
}

// convert row id to key, filling unused bits to zero
inline uint64_t row_to_key(int row) {
	return uint64_t(row)<<(64-ROW_BITS);
}

struct str_with_id {
	std::string  str;
	int64_t id;
};

struct dual_string {
	std::string kstr;
	std::string vstr;
};

struct kv_pair {
	int64_t     id;
	uint64_t    key;
	dual_string value;
};


struct dstr_with_id {
	dual_string dstr;
	int64_t     id;
};

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

}


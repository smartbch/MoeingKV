#pragma once
#include <string>

namespace moeingkv {

enum __const_t {
	ROW_COUNT = 256,
	ROW_BITS = 12,

	LEAF_BITS = 24,
	LEAF_MASK = (1<<LEAF_BITS) - 1,
	U64_COUNT = (1<<LEAF_BITS)/64,

	HASH_COUNT = 8,
	BITS_PER_ENTRY = 20,
};

struct selector64 {
	int n;
	uint64_t mask;
	selector64(int vault_lsb) {
		n = vault_lsb/64;
		mask = uint64_t(1) << uint64_t(vault_lsb%64);
	}
};

struct seeds {
	uint64_t u64[HASH_COUNT];
};

inline int row_from_key(uint64_t key) {
	return int(key>>(64-ROW_BITS)) % ROW_COUNT;
}

inline uint64_t row_to_key(int row) {
	return uint64_t(row)<<(64-ROW_BITS);
}

struct str_with_id {
	std::string  str;
	int64_t id;
};

struct dual_string {
	std::string first;
	std::string second;
	int size() const {
		return first.size() + second.size();
	}
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


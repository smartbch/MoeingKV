#pragma once
#include <string>

namespace moeingkv {

enum __const_t {
	ROW_COUNT = 256,
	ROW_BITS = 12,

	LEAF_BITS = 24,
	LEAF_MASK = (1<<LEAF_BITS) - 1,
	BYTE_COUNT = (1<<LEAF_BITS)/8,
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

}


#pragma once
#include <stdint.h>
#include <array>
#include <vector>
#include "cpp-btree-1.0.1/btree_map.h"

namespace chainkv {

class bitarray {
	enum {
		leaf_bits = 24,
		leaf_mask = (1<<leaf_bits) - 1,
		byte_count = (1<<leaf_bits)/8,
	};
	typedef std::array<uint8_t, byte_count> arr_t;
	btree::btree_map<int64_t, arr_t*> tree;
	bool modify_and_return_old(int64_t pos, bool set, bool clear) {
		auto key = pos>>leaf_bits;
		pos = pos & leaf_mask;
		auto iter = tree.find(key);
		if(iter == tree.end()) {
			tree.insert(std::make_pair(key, new arr_t));
			iter = tree.find(key);
		}
		auto arr_ptr = iter->second;
		auto byte_offset = pos/8;
		auto byte_mask = uint8_t(1) << (pos%8);
		auto old = (arr_ptr->at(byte_offset) & byte_mask) != 0;
		if(set) {
			arr_ptr->at(byte_offset) |= byte_mask;
		}
		if(clear) {
			arr_ptr->at(byte_offset) &= ~byte_mask;
		}
		return old;
	}
public:
	bool get(int64_t pos) {
		return modify_and_return_old(pos, false, false);
	}
	bool set(int64_t pos) {
		return modify_and_return_old(pos, true, false);
	}
	bool clear(int64_t pos) {
		return modify_and_return_old(pos, false, true);
	}
	void prune_till(int64_t pos) {
		std::vector<int64_t> pos_list;
		auto iter = tree.upper_bound(pos>>leaf_bits);
		while(iter != tree.end()) {
			delete iter->second;
			pos_list.push_back(iter->first);
			iter--;
		}
		for(auto iter = pos_list.begin(); iter != pos_list.end(); iter++) {
			tree.erase(*iter);
		}
	}
};

}

#pragma once
#include <stdio.h>
#include <string.h>
#include <array>
#include <atomic>
#include "cpp-btree-1.0.1/btree_map.h"
#include "log.h"

namespace moeingkv {

// A very large vector of atomic pointers. Its internal memory is not continuous. Instead,
// its uses multi-level tree structure. The ununsed parts (null pointers) are not allocated.
// Its head part can be pruned.
template<typename T>
class large_atomic_ptr_vector {
	struct leaf_node {
		std::array<std::atomic<T*>, 256> ptr_arr;
		~leaf_node() {
			for(int64_t i=0; i<ptr_arr.size(); i++) {
				delete ptr_arr[i].load();
			}
		}
		T* get(int64_t i) const {
			return ptr_arr[i].load();
		}
		void set(int64_t i, T* new_ptr) {
			return ptr_arr[i].store(new_ptr);
		}
		void prune_till(int64_t n) {
			for(int64_t i=0; i<n; i++) {
				auto ptr = ptr_arr[i].exchange(nullptr);
				delete ptr;
			}
		}
	};

	template<typename SUB, int L>
	struct node {
		std::array<std::atomic<SUB*>, 256> ptr_arr;
		~node() {
			for(int64_t i=0; i<ptr_arr.size(); i++) {
				delete ptr_arr[i].load();
			}
		}
		static constexpr int64_t shift() {
			return L*8;
		}
		static constexpr int64_t mask() {
			return (int64_t(1)<<shift())-1;
		}
		T* get(int64_t i) const {
			auto ptr = ptr_arr[i>>shift()].load();
			if(ptr == nullptr) return nullptr;
			return ptr->get(i&mask());
		}
		void set(int64_t i, T* new_ptr) {
			auto ptr = ptr_arr[i>>shift()].load();
			if(ptr == nullptr) {
				ptr = new SUB;
				ptr_arr[i>>shift()].store(ptr);
			}
			return ptr->set(i&mask(), new_ptr);
		}
		void prune_till(int64_t n) {
			auto n_sh = n >> shift();
			for(int64_t i=0; i < n_sh; i++) {
				auto ptr = ptr_arr[i].exchange(nullptr);
				delete ptr;
			}
			auto ptr = ptr_arr[n_sh].load();
			ptr->prune_till(n&mask());
		}
	};
	typedef node<leaf_node, 1> mid1_node; //T* count: 2**16
	typedef node<mid1_node, 2> mid2_node; //T* count: 2**24
	typedef node<mid2_node, 3> top_node; //T* count: 2**32
public:
	top_node top;
	T* get(int64_t i) const {
		return top.get(i);
	}
	void set(int64_t i, T* new_ptr) {
		top.set(i, new_ptr);
	}
	void prune_till(int64_t pos) {
		top.prune_till(pos);
	}
};

// A virtually 2**54-entry bitarray. Its beginning part can be pruned. 
// The default value for the bits is 0.
class bitarray: public ds_with_log {
	enum {
		LEAF_BITS = 24, // 16 million bits, 2MByte
		LEAF_MASK = (1<<LEAF_BITS) - 1,
		U64_COUNT = (1<<LEAF_BITS)/64,
	};
	typedef std::array<std::atomic_ullong, U64_COUNT> arr_t;

	static arr_t* get_empty_arr() {
		auto a = new arr_t;
		for(int i = 0; i < a->size(); i++) {
			a->at(i).store(0);
		}
		return a;
	}

	large_atomic_ptr_vector<arr_t> vec_of_arr; 

public:
	bitarray(): vec_of_arr() {}
	bitarray(const bitarray& other) = delete;
	bitarray& operator=(const bitarray& other) = delete;
	bitarray(bitarray&& other) = delete;
	bitarray& operator=(bitarray&& other) = delete;

	bool get(int64_t pos) const {
		auto arr_ptr = vec_of_arr.get(pos>>LEAF_BITS);
		if(arr_ptr == nullptr) return false;
		selector64 sel(pos&LEAF_MASK);
		return (arr_ptr->at(sel.n).load() & sel.mask) != 0;
	}
private:
	void modify(int64_t pos, bool set, bool clear) {
		auto arr_ptr = vec_of_arr.get(pos>>LEAF_BITS);
		if(arr_ptr == nullptr) {
			arr_ptr = get_empty_arr();
			vec_of_arr.set(pos>>LEAF_BITS, arr_ptr);
		}
		selector64 sel(pos&LEAF_MASK);
		if(set) {
			arr_ptr->at(sel.n).fetch_or(sel.mask);
		}
		if(clear) {
			arr_ptr->at(sel.n).fetch_and(~sel.mask);
		}
	}
public:
	void set(int64_t pos) {
		modify(pos, true, false);
	}
	void clear(int64_t pos) {
		modify(pos, false, true);
	}
	void log_set(int64_t pos) { // positive for set
		log_i64(pos);
	}
	void log_clear(int64_t pos) { //negative for clear
		log_i64(-pos);
	}
	void log_rw_vault_log_size(int64_t size) {
		log_i64(RW_VAULT_LOG_SIZE_TAG);
		log_i64(size);
	}
	void prune_till(int64_t pos) {
		vec_of_arr.prune_till(pos>>LEAF_BITS);
	}
	bool load_data_from_logs(const std::vector<int>& file_list, int64_t* rw_vault_log_size) {
		for(int i=0; i<file_list.size(); i++) {
			std::string fname = log_dir+"/"+std::to_string(file_list[i]);
			std::ifstream fin;
			fin.open(fname.c_str(), std::ios::in | std::ios::binary);
			if(!fin.is_open()) {
				std::cerr<<"Failed to open file "<<fname<<std::endl;
				return false;
			}
			int64_t i64;
			for(;;) {
				auto state = read_i64(fin, &i64);
				if((state & std::ios::eofbit ) != 0) {
					break;
				}
				if((state & std::ios::failbit ) != 0) {
					std::cerr<<"Failed to read file "<<fname<<std::endl;
					return false;
				}
				if(i64 == RW_VAULT_LOG_SIZE_TAG) {
					state = read_i64(fin, rw_vault_log_size);
					if((state & std::ios::failbit ) != 0) {
						std::cerr<<"Failed to read file "<<fname<<std::endl;
						return false;
					}
				} else if(i64 > 0) { // positive for set
					set(i64);
				} else { //negative for clear
					clear(-i64);
				}
			}
		}
		return true;
	}
};

}

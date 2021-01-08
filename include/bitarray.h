#pragma once
#include <stdio.h>
#include <string.h>
#include <array>
#include <atomic>
#include "cpp-btree-1.0.1/btree_map.h"
#include "log.h"

namespace moeingkv {

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
			if(n_sh < ptr_arr.size()) {
				auto ptr = ptr_arr[n_sh].load();
				ptr->prune_till(n&mask());
			}
		}
	};
	typedef node<leaf_node, 1> mid1_node;
	typedef node<mid1_node, 2> mid2_node;
	typedef node<mid2_node, 3> top_node;
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

// A virtually infinite bitarray. Its beginning part can be pruned. 
// The default value for the bits is 0.
class bitarray: public ds_with_log {
	typedef std::array<std::atomic_ullong, U64_COUNT> arr_t;

	static arr_t* get_empty_arr() {
		auto a = new arr_t;
		for(int i=0; i<a->size(); i++) {
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
	void log_set(int64_t pos) {
		log_i64(pos);
	}
	void log_clear(int64_t pos) {
		log_i64(-pos);
	}
	void prune_till(int64_t pos) {
		vec_of_arr.prune_till(pos>>LEAF_BITS);
	}
	bool load_data_from_logs(const std::vector<int>& file_list) {
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
				if(i64 > 0) { // positive for set
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

#pragma once
#include "page.h"
#include "u64vec.h"

namespace chainkv {

enum __const_t {
	ROW_COUNT = 256,
	ROW_BITS = 12,
};

struct point {
	uint8_t col;
	int row;
};

inline int row_from_key(uint64_t key) {
	return int(key>>(64-ROW_BITS)) % ROW_COUNT;
}

inline uint64_t row_to_key(int row) {
	return uint64_t(row)<<(64-ROW_BITS);
}

class rawkv {
	btree::btree_map<uint64_t, std::string> fresh_map[256];
	int             col_fd[256];
	u64vec          col_index[256];
	bloomfilter256* bf256[ROW_COUNT];
	point           mov_pt;
	bitarray        del_mark;
	seeds*          seeds_for_bloom;
public:
	enum kv_status {
		IN_MEM = 1,
		ON_DISK = 2,
		NOT_FOUND = 3,
	};
	rawkv(int count_for_bloom, seeds* s): seeds_for_bloom(s) {
		mov_pt.col = 0;
		mov_pt.row = 0;
		for(int i=0; i<ROW_COUNT; i++) {
			bf256[i] = new bloomfilter256(count_for_bloom, seeds_for_bloom);
		}
	}
	~rawkv() {
		for(int i=0; i<ROW_COUNT; i++) {
			delete bf256[i];
		}
	}
	rawkv(const rawkv& other) = delete;
	rawkv& operator=(const rawkv& other) = delete;
	rawkv(rawkv&& other) = delete;
	rawkv& operator=(rawkv&& other) = delete;
	
	bool get_in_cells(uint64_t key, std::string* value, int64_t* id) {
		int row = row_from_key(key);
		uint8_t gap_col = mov_pt.col;
		if(row >= mov_pt.row) {
			gap_col++;
		}
		auto list = bf256[row]->get_pos_list(key, gap_col);
		if(list.size == 0) {
			return false;
		}
		for(int i=0; i<list.size; i++) {
			uint8_t col = list.data[i];
			ssize_t pageid = col_index[col].binary_search(key);
			if(pageid < 0) {
				continue; 
			}
			auto pageoff = pageid * PAGE_SIZE;
			page pg;
			auto sz = pread(col_fd[col], pg.data(), PAGE_SIZE, pageoff);
			assert(sz == PAGE_SIZE);
			kv_pair kv = pg.lookup(key);
			if(kv.id != -1) {
				if(del_mark.get(kv.id)) { //was deleted
					return false;
				}
				*value = kv.value;
				*id = kv.id;
				return true;
			}
		}
		return false;
	}
	void fresh_insert(uint64_t key, const std::string& value) {
		int row = row_from_key(key);
		fresh_map[row].insert(make_pair(key, value));
	}
	void fresh_erase(uint64_t key) {
		int row = row_from_key(key);
		fresh_map[row].erase(key);
	}
	void delete_id(int64_t id) {
		del_mark.clear(id);
	}
	kv_status get(uint64_t key, std::string* value, int64_t* id) {
		int row = row_from_key(key);
		auto iter = fresh_map[row].find(key);
		if(iter != fresh_map[row].end()) {
			*id = iter->first;
			*value = iter->second;
			return IN_MEM;
		}
		bool ok = get_in_cells(key, value, id);
		if(ok) {
			return ON_DISK;
		}
		return NOT_FOUND;
	}
	void move_cell(uint8_t col, int row, kv_producer* kv_prod) {
		ssize_t start = col_index[col].binary_search(row_to_key(row)) * PAGE_SIZE;
		if(start < 0) start = 0;
		ssize_t end = col_index[col].binary_search(row_to_key(row+1)) * PAGE_SIZE;
		if(end < 0) end = 0; //todo, maybe bug here
		kv_reader reader(start, end, col_fd[col], &del_mark);
		merged_kv_producer merger(&reader, kv_prod);
		kv_packer packer(col_fd[col+1], bf256[row], col+1);
		while(merger.has_more()) {
			packer.consume(merger.produce());
		}
		packer.flush();
	}
};

}

#pragma once
#include <mutex>
#include "u64vec.h"
#include "freshmap.h"
#include "cpp-btree-1.0.1/btree_set.h"

namespace moeingkv {

struct point {
	int col;
	int row;
};

inline uint64_t hashstr(const std::string& str, uint64_t seed) {
	return XXHash64::hash(str.data(), str.size(), seed);
}

class internalkv {
	freshmap        fmap;
	int             col_fd[256];
	u64vec          col_index[256];
	bloomfilter256* bf256[ROW_COUNT];
	point           mov_pt;
	bitarray        del_mark;
	seeds*          seeds_for_bloom;

	bloomfilter*        tmp_bf;
	bloomfilter256*     new_bf256;
	bloomfilter256*     old_bf256;
	freshmap::i2str_map new_map;
	std::atomic_bool         ok_to_compact;
public:
	internalkv(int count_for_bloom, seeds* s): seeds_for_bloom(s) {
		mov_pt.col = 0;
		mov_pt.row = 0;
		for(int i=0; i<ROW_COUNT; i++) {
			bf256[i] = new bloomfilter256(count_for_bloom, seeds_for_bloom);
		}
	}
	~internalkv() {
		for(int i=0; i<ROW_COUNT; i++) {
			delete bf256[i];
		}
	}
	internalkv(const internalkv& other) = delete;
	internalkv& operator=(const internalkv& other) = delete;
	internalkv(internalkv&& other) = delete;
	internalkv& operator=(internalkv&& other) = delete;

	bool is_ok_to_compact() {
		return ok_to_compact.load();
	}
	void commit_compaction() {
		if(new_bf256 != nullptr) {
			old_bf256 = bf256[mov_pt.row%ROW_COUNT];
			bf256[mov_pt.row%ROW_COUNT] = new_bf256;
		}
		if(tmp_bf != nullptr) {
			bf256[mov_pt.row%ROW_COUNT]->assign_at(uint8_t(mov_pt.col), tmp_bf);
		}
		fmap.swap(mov_pt.row%ROW_COUNT, new_map);
		mov_pt.row++;
		if(mov_pt.row == ROW_COUNT) {
			mov_pt.row = 0;
			mov_pt.col++;
			del_mark.switch_log(mov_pt.col);
		}
		fmap.switch_log(mov_pt.col*ROW_COUNT + mov_pt.row);
	}
	
	bool get(uint64_t key, const std::string& first_value, str_with_id* out) {
		if(fmap.get(key, first_value, out, &del_mark)) {
			return true;
		}
		int row = row_from_key(key);
		uint8_t gap_col = uint8_t(mov_pt.col);
		if(row >= mov_pt.row) {
			gap_col++;
		}
		auto list = bf256[row]->get_pos_list(key, gap_col);
		if(list.size == 0) {
			return false;
		}
		for(int i=0; i<list.size; i++) {
			uint8_t col = list.data[i];
			ssize_t pageid = col_index[col].search(key);
			if(pageid < 0) {
				continue; 
			}
			auto pageoff = pageid * PAGE_SIZE;
			page pg;
			auto sz = pread(col_fd[col], pg.data(), PAGE_SIZE, pageoff);
			assert(sz == PAGE_SIZE);
			bool ok = pg.lookup(key, first_value, out, &del_mark);
			if(ok) return true;
		}
		return false;
	}
	void update(const btree::btree_set<int64_t>& del_list,
		const btree::btree_map<std::string, str_with_id>& new_map, uint64_t seed) {
		for(auto iter = del_list.begin(); iter != del_list.end(); iter++) {
			del_mark.set(*iter);
		}
		for(auto iter = new_map.begin(); iter != new_map.end(); iter++) {
			uint64_t hashkey = hashstr(iter->first, seed);
			dstr_with_id data;
			data.dstr.first = iter->first;
			data.dstr.second = iter->second.str;
			data.id = iter->second.id;
			fmap.insert(hashkey, data);
		}
		//TODO record metainfo (including log files' sizes)
	}
	void move_cell(uint8_t col, int row) {
		ssize_t start = col_index[col].search(row_to_key(row)) * PAGE_SIZE;
		if(start < 0) start = 0;
		ssize_t end = col_index[col].search(row_to_key(row+1)) * PAGE_SIZE;
		if(end < 0) end = 0; //todo, maybe bug here
		kv_reader reader(start, end, col_fd[col], &del_mark);
		auto prod = fmap.get_kv_producer(row, &del_mark);
		merged_kv_producer merger(&reader, &prod);
		kv_packer packer(col_fd[col+1], tmp_bf, col+1);
		while(merger.valid()) {
			packer.consume(merger.produce());
		}
		packer.flush();
	}
};

}

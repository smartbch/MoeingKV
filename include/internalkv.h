#pragma once
#include <mutex>
#include "u64vec.h"
#include "freshmap.h"
#include "cpp-btree-1.0.1/btree_set.h"

namespace moeingkv {

inline uint64_t hashstr(const std::string& str, uint64_t seed) {
	return XXHash64::hash(str.data(), str.size(), seed);
}

class compactor {
	freshmap*       new_map;
	freshmap*       ro_map;
	u64vec*         vault_index;
	int             vault_fd;
	bitarray*       del_mark;

	pointer_array<bloomfilter256, ROW_COUNT>* bf256;
	std::atomic_bool done;
};

class internalkv {
	int             young_vault;
	int             old_vault;
	freshmap*       rw_map;
	freshmap*       ro_map;
	int             vault_fd[256];
	u64vec          vault_index[256];
	bitarray        del_mark;
	seeds*          seeds_for_bloom;

	pointer_array<bloomfilter256, ROW_COUNT> bf256;
public:
	internalkv(int count_for_bloom, seeds* s): seeds_for_bloom(s) {
		for(int i=0; i<ROW_COUNT; i++) {
			bf256.replace(i, new bloomfilter256(count_for_bloom, seeds_for_bloom));
		}
		rw_map = new freshmap;
		ro_map = new freshmap;
	}
	~internalkv() {
		delete rw_map;
		delete ro_map;
	}
	internalkv(const internalkv& other) = delete;
	internalkv& operator=(const internalkv& other) = delete;
	internalkv(internalkv&& other) = delete;
	internalkv& operator=(internalkv&& other) = delete;

	bool get(uint64_t key, const std::string& first_value, str_with_id* out) {
		if(rw_map->get(key, first_value, out, &del_mark)) {
			return true;
		}
		if(ro_map->get(key, first_value, out, &del_mark)) {
			return true;
		}
		bits256 mask;
		auto row = row_from_key(key);
		auto bf_ptr = bf256.get_ptr(row);
		bf_ptr->get_mask(key, mask);
		bf256.return_ptr(bf_ptr);
		std::vector<uint8_t> pos_list;
		for(int i = 0; i < 255; i++) {
			auto pos = young_vault - i;
			if(mask.get(pos)) {
				pos_list.push_back(uint8_t(pos));
			}
		}
		if(pos_list.size() == 0) {
			return false;
		}
		for(int i=0; i<pos_list.size(); i++) {
			uint8_t vault_lsb = pos_list[i];
			ssize_t pageid = vault_index[vault_lsb].search(key);
			if(pageid < 0) {
				continue; 
			}
			auto pageoff = pageid * PAGE_SIZE;
			page pg;
			auto sz = pread(vault_fd[vault_lsb], pg.data(), PAGE_SIZE, pageoff);
			assert(sz == PAGE_SIZE);
			bool ok = pg.lookup(key, first_value, out, &del_mark);
			if(ok) return true;
		}
		return false;
	}
	//void update(const btree::btree_set<int64_t>& del_list,
	//	const btree::btree_map<std::string, str_with_id>& new_map, uint64_t seed) {
	//	for(auto iter = del_list.begin(); iter != del_list.end(); iter++) {
	//		del_mark.set(*iter);
	//	}
	//	for(auto iter = new_map.begin(); iter != new_map.end(); iter++) {
	//		uint64_t hashkey = hashstr(iter->first, seed);
	//		dstr_with_id data;
	//		data.dstr.first = iter->first;
	//		data.dstr.second = iter->second.str;
	//		data.id = iter->second.id;
	//		fmap.add(hashkey, data);
	//	}
	//	//TODO record metainfo (including log files' sizes)
	//}
	//void move_cell(uint8_t vault_lsb, int row) {
	//	ssize_t start = vault_index[vault_lsb].search(row_to_key(row)) * PAGE_SIZE;
	//	if(start < 0) start = 0;
	//	ssize_t end = vault_index[vault_lsb].search(row_to_key(row+1)) * PAGE_SIZE;
	//	if(end < 0) end = 0; //todo, maybe bug here
	//	kv_reader reader(start, end, vault_fd[vault_lsb], &del_mark);
	//	auto prod = fmap.get_kv_producer(row, &del_mark);
	//	merged_kv_producer merger(&reader, &prod);
	//	kv_packer packer(vault_fd[vault_lsb+1], bf256[row], vault_lsb+1);
	//	while(merger.valid()) {
	//		packer.consume(merger.produce());
	//	}
	//	packer.flush();
	//}
};

}

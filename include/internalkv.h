#pragma once
#include <mutex>
#include "u64vec.h"
#include "ptr_for_rent.h"
#include "freshmap.h"
#include "sharded_map.h"
#include "cpp-btree-1.0.1/btree_set.h"

namespace moeingkv {

class compactor {
	friend class internalkv;
	freshmap*       new_map;
	freshmap*       ro_map;
	u64vec*         old_vault_index;
	u64vec*         new_vault_index;
	int             new_vault_fd;
	int             old_vault_fd;
	uint8_t         new_vault_lsb;
	bitarray*       del_mark;
	seeds*          seeds_for_bloom;

	std::array<ptr_for_rent<bloomfilter256>, ROW_COUNT>* bf256arr;
	std::atomic_bool done;
	size_t check_bloomfilter_size(int row) {
		size_t size;
		bf256arr->at(row).rent_const([&size](const bloomfilter256* curr_bf) {
			size = curr_bf->size();
		});
		if(size < 2 * BITS_PER_ENTRY * ro_map->size_at_row(row)) {
			bloomfilter256* bf; // an enlarged bloomfilter
			bf256arr->at(row).rent_const([&bf](const bloomfilter256* curr_bf) {
				bf = curr_bf->double_sized();
			});
			bf256arr->at(row).replace(bf);
			size *= 2;
		}
		return size;
	}
	void compact_row(int row) {
		size_t bloom_size = check_bloomfilter_size(row);
		bloomfilter new_bf(bloom_size, seeds_for_bloom);
		ssize_t start = old_vault_index->search(row_to_key(row)) * PAGE_SIZE;
		if(start < 0) return;
		ssize_t end = old_vault_index->search(row_to_key(row+1)) * PAGE_SIZE;
		assert(end >= 1);
		kv_reader reader(start, end, old_vault_fd, del_mark);
		auto prod = ro_map->get_kv_producer(row, del_mark);
		merged_kv_producer merger(&reader, &prod);
		kv_packer packer(new_vault_fd, &new_bf, new_vault_index);
		int64_t total = 0;
		bool bloom_is_full = false;
		while(merger.valid()) {
			auto kv = merger.produce();
			if(bloom_is_full) {
				new_map->add(kv.key, dstr_with_id{.dstr=kv.value, .id=kv.id});
				continue;
			}
			if(bloom_size < BITS_PER_ENTRY * total) {
				packer.flush();
				bloom_is_full = true;
			}
			if(!packer.can_consume(kv)) {
				packer.flush();
				while(merger.in_middle_of_same_key()) {
					new_map->add(kv.key, dstr_with_id{.dstr=kv.value, .id=kv.id});
					kv = merger.produce();
				}
			}
			packer.consume(kv);
			total++;
		}
		if(!bloom_is_full) {
			packer.flush();
		}

		bf256arr->at(row).rent([&new_bf, this](bloomfilter256* curr_bf) {
			curr_bf->assign_at(this->new_vault_lsb, &new_bf);
		});
	}
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

	sharded_map<1024> cache;
	std::array<ptr_for_rent<bloomfilter256>, ROW_COUNT> bf256arr;
	int64_t next_id;
public:
	internalkv(int count_for_bloom, seeds* s): seeds_for_bloom(s) {
		for(int i=0; i<ROW_COUNT; i++) {
			bf256arr[i].replace(new bloomfilter256(count_for_bloom, seeds_for_bloom));
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
		if(cache.get(key, first_value, out)) {
			if(out->id < 0) return false;
			if(!del_mark.get(out->id)) return true;
		}
		bool res = _get(key, first_value, out);
		if(res) {
			cache.add(key, first_value, out->str, out->id);
		} else {
			cache.add(key, first_value, "", -1);
		}
		return res;
	}
	bool _get(uint64_t key, const std::string& first_value, str_with_id* out) {
		if(rw_map->get(key, first_value, out, &del_mark)) {
			return true;
		}
		if(ro_map->get(key, first_value, out, &del_mark)) {
			return true;
		}
		bits256 mask;
		auto row = row_from_key(key);
		bf256arr[row].rent_const([&key, &mask](const bloomfilter256* bf_ptr) {
			bf_ptr->get_mask(key, mask);
		});
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
			if(ok) {
				return true;
			}
		}
		return false;
	}
	void update(const btree::btree_multimap<uint64_t, dstr_with_id>& new_map) {
		str_with_id str_and_id;
		for(auto iter = new_map.begin(); iter != new_map.end(); iter++) {
			bool is_del = iter->second.id < 0;
			if(is_del) {
				bool found_it = get(iter->first, iter->second.dstr.first, &str_and_id);
				if(found_it) {
					cache.mark_as_deleted(iter->first, iter->second.dstr.first);
					del_mark.clear(str_and_id.id); //TODO walog
				}
			} else {
				auto v = iter->second;
				v.id = next_id++;
				rw_map->add(iter->first, v); //TODO walog
			}
		}
	}
};

}

#pragma once
#include <mutex>
#include <fcntl.h>
#include "u64vec.h"
#include "ptr_for_rent.h"
#include "vault_in_mem.h"
#include "sharded_cache.h"
#include "cpp-btree-1.0.1/btree_set.h"

namespace moeingkv {

typedef std::array<ptr_for_rent<bloomfilter256>, ROW_COUNT> bf256arr_t;

class compactor {
	friend class internalkv;
	vault_in_mem*    wo_vault; // a write-only vault
	vault_in_mem*    ro_vault; // a read-only vault
	u64vec*          old_vault_index;
	int              old_vault_fd;
	u64vec*          new_vault_index;
	int              new_vault_fd;
	uint8_t          new_vault_lsb;
	bitarray*        del_mark;
	seeds*           seeds_for_bloom;
	bf256arr_t*      bf256arr;
	std::atomic_bool done;
	// check the size of the bloomfilter at 'row', if it is too small, replaced it with a double-sized one
	size_t check_bloomfilter_size(int row) {
		size_t size;
		bloomfilter256* bf = nullptr; // a 2x enlarged bloomfilter
		bf256arr->at(row).rent_const([&size, &bf, this, row](const bloomfilter256* curr_bf) {
			size = curr_bf->size();
			if(size < 2 * BITS_PER_ENTRY * this->ro_vault->size_at_row(row)) {
				bf = curr_bf->double_sized();
				size *= 2;
			}
		});
		if(bf != nullptr) {
			bf256arr->at(row).replace(bf);
		}
		return size;
	}
	// compact one row of old vault and one row of ro_vault into one row of new vault.
	// some entries which cannot be compacted, will be inserted to wo_vault. 
	void compact_row(int row) {
		size_t bloom_size = check_bloomfilter_size(row);
		bloomfilter new_bf(bloom_size, seeds_for_bloom);
		ssize_t start = old_vault_index->search(row_to_key(row)) * PAGE_SIZE;
		if(start < 0) return;
		ssize_t end = old_vault_index->search(row_to_key(row+1)) * PAGE_SIZE;
		assert(end >= 1);
		kv_reader reader(start, end, old_vault_fd, del_mark);
		auto prod = ro_vault->get_kv_producer(row, del_mark);
		merged_kv_producer merger(&reader, &prod);
		kv_packer packer(new_vault_fd, &new_bf, new_vault_index);
		int64_t packed_num = 0;
		bool bloom_is_full = false;
		while(merger.valid()) {
			auto kv = merger.produce();
			if(bloom_is_full) {// bloomfilter is full, so new entries go into wo_vault
				wo_vault->add(kv.key, dstr_with_id{.dstr=kv.value, .id=kv.id});
				continue;
			}
			if(!packer.can_consume(kv)) { //enough kv pairs for one page
				packer.flush(); //flush the kv pairs to disk
				while(merger.in_middle_of_same_key()) {// same key cannot span two pages
					wo_vault->add(kv.key, dstr_with_id{.dstr=kv.value, .id=kv.id});
					kv = merger.produce();
				}
			}
			packer.consume(kv);
			packed_num++;
			if(bloom_size < BITS_PER_ENTRY * packed_num) {
				packer.flush();
				bloom_is_full = true;
			}
		}
		packer.flush(); // it is a nop if already flushed

		bf256arr->at(row).rent([&new_bf, this](bloomfilter256* curr_bf) {
			curr_bf->assign_at(this->new_vault_lsb, &new_bf);
		});
	}
	void compact() {
		for(int i=0; i<ROW_COUNT; i++) {
			compact_row(i);
		}
		done.store(true);
	}
};

class internalkv {
	std::string     data_dir;
	int             youngest_vault;
	int             oldest_vault;
	vault_in_mem*   rw_vault;
	vault_in_mem*   ro_vault;
	int             vault_fd[VAULT_COUNT];
	u64vec          vault_index[VAULT_COUNT];
	bitarray        del_mark;
	seeds           seeds_for_bloom;

	compactor       compactor;

	sharded_cache<CACHE_SHARD_COUNT> cache;
	std::array<ptr_for_rent<bloomfilter256>, ROW_COUNT> bf256arr;
	int64_t next_id;

	//void set_log_dir(const std::string& dir) {
	//bool open_log(int num) {
	void init_compactor() {
		compactor.ro_vault = ro_vault;
		compactor.del_mark = &del_mark;
		compactor.seeds_for_bloom = &seeds_for_bloom;
		compactor.bf256arr = &bf256arr;

		compactor.wo_vault = new vault_in_mem;
		compactor.wo_vault->set_log_dir(data_dir+MEM_VAULT_LOG_DIR);
		compactor.wo_vault->open_log(youngest_vault+4); // new log for mem vault is created

		compactor.new_vault_lsb = (youngest_vault+1)%VAULT_COUNT;
		compactor.new_vault_index = &vault_index[compactor.new_vault_lsb];
		auto new_fname = data_dir+"/"+DISK_VAULT_DIR+"/"+std::to_string(youngest_vault+1);
		compactor.new_vault_fd = open(new_fname.c_str(), O_RDWR); // new disk vault is created
		vault_fd[compactor.new_vault_lsb] = compactor.new_vault_fd;

		compactor.old_vault_index = &vault_index[oldest_vault%VAULT_COUNT];
		compactor.old_vault_fd = vault_fd[oldest_vault%VAULT_COUNT];
		compactor.done.store(false);
	}
	//void save_meta() {
	//	std::ofstream new_meta_file;
	//	auto orig_meta_fname = data_dir+"/"+META_FILE;
	//	auto new_meta_fname = orig_meta_fname+".new";
	//	new_meta_file.open(new_meta_fname, std::ios::trunc | std::ios::app | std::ios::binary);
	//	print_meta_to_file(new_meta_file);
	//	new_meta_file.close();
	//	rename(new_meta_fname.c_str(), orig_meta_fname.c_str());
	//}
	//void print_meta_to_file(std::ofstream& fout) {
	//	fout<<"youngest_vault "<<youngest_vault<<std::endl;
	//	fout<<"oldest_vault "<<youngest_vault<<std::endl;
	//	fout<<"rw_vault_log_size "<<rw_vault->log_file_size()<<std::endl;
	//	fout<<"del_log_size "<<del_mark.log_file_size()<<std::endl;
	//	fout<<"bloomfilter_sizes"<<std::endl;
	//	for(int row=0; row<ROW_COUNT; row++) {
	//		bf256arr.at(row).rent_const([&fout](const bloomfilter256* curr_bf) {
	//			auto size = curr_bf->size();
	//			fout<<size<<std::endl;
	//		});
	//	}
	//}
	void done_compaction() {
		ro_vault->remove_log();
		delete ro_vault;
		ro_vault = rw_vault;
		rw_vault = compactor.wo_vault;

		close(vault_fd[(oldest_vault-1)%VAULT_COUNT]);
		remove_file(data_dir+"/"+DISK_VAULT_DIR+"/"+std::to_string(oldest_vault-1));
		remove_file(data_dir+"/"+DEL_LOG_DIR+"/"+std::to_string(oldest_vault-1));

		compactor.done.store(false);
	}
public:
	internalkv(int count_for_bloom, const seeds& s): seeds_for_bloom(s) {
		for(int i=0; i<ROW_COUNT; i++) {
			bf256arr[i].replace(new bloomfilter256(count_for_bloom, &seeds_for_bloom));
		}
		rw_vault = new vault_in_mem;
		ro_vault = new vault_in_mem;
	}
	~internalkv() {
		delete rw_vault;
		delete ro_vault;
	}
	internalkv(const internalkv& other) = delete;
	internalkv& operator=(const internalkv& other) = delete;
	internalkv(internalkv&& other) = delete;
	internalkv& operator=(internalkv&& other) = delete;

	bool lookup(uint64_t key, const std::string& first_value, str_with_id* out) {
		if(cache.lookup(key, first_value, out)) {
			if(out->id < 0) return false;
			if(!del_mark.get(out->id)) return true;
		}
		bool res = _lookup(key, first_value, out);
		if(res) {
			cache.add(key, first_value, out->str, out->id);
		} else {
			cache.add(key, first_value, "", -1);
		}
		return res;
	}
private:
	bool _lookup(uint64_t key, const std::string& first_value, str_with_id* out) {
		if(rw_vault->lookup(key, first_value, out, &del_mark)) {
			return true;
		}
		if(ro_vault->lookup(key, first_value, out, &del_mark)) {
			return true;
		}
		bitslice mask;
		auto row = row_from_key(key);
		bf256arr[row].rent_const([&key, &mask](const bloomfilter256* bf_ptr) {
			bf_ptr->get_mask(key, mask);
		});
		std::vector<uint8_t> pos_list;
		for(int i = 0; i < 255; i++) {
			auto pos = youngest_vault - i;
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
public:
	bool can_start_compaction(); //TODO
	void update(btree::btree_multimap<uint64_t, dstr_with_id>* new_vault) {
		str_with_id str_and_id;
		std::vector<uint64_t> del_ids;
		for(auto iter = new_vault->begin(); iter != new_vault->end(); iter++) {
			bool is_del = iter->second.id < 0;
			if(is_del) {
				bool found_it = lookup(iter->first, iter->second.dstr.kstr, &str_and_id);
				if(found_it) {
					del_ids.push_back(str_and_id.id);
					del_mark.log_set(str_and_id.id);
				}
			} else {
				auto& v = iter->second;
				v.id = next_id++;
				cache.add(iter->first, v.dstr.kstr, v.dstr.vstr, v.id);
				rw_vault->log_add_kv(iter->first, v);
			}
		}
		del_mark.log_clear(next_id);
		if(can_start_compaction()) {
			youngest_vault++;
			oldest_vault++;
			rw_vault->flush_log();
			// new log for del_mark is created, which indicates id-switch
			del_mark.switch_log(youngest_vault+1);
			done_compaction();
			init_compactor();
		} else {
			rw_vault->flush_log();
			del_mark.flush_log();
		}

		for(int i=0; i < del_ids.size(); i++) {
			del_mark.set(del_ids[i]);
		}
		for(auto iter = new_vault->begin(); iter != new_vault->end(); iter++) {
			if(iter->second.id >= 0) {
				rw_vault->add(iter->first, iter->second);
			}
		}
		del_mark.clear(next_id);
	}
};

}

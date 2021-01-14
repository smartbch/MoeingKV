#include "./include/log.h"
#include "./include/bitarray.h"
#include "./include/bloomfilter.h"
#include "./include/vault_in_mem.h"
#include "./include/u64vec.h"
#include "./include/internalkv.h"
#include "./include/moeingkv.h"

int main() {
	moeingkv::bitarray ba;
	moeingkv::vault_in_mem m;
	moeingkv::u64vec v;
	moeingkv::bloomfilter256 bf(100, nullptr);
	return 0;
}	

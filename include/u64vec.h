#pragma once
#include "stdint.h"
#include <array>
#include <vector>

namespace moeingkv {

class u64vec {
	enum {
		SEGMENT_SIZE = 128*1024/8,
		BINSEARCH_THRES = 100,
	};
	typedef std::array<uint64_t, SEGMENT_SIZE> u64arr;
	std::vector<u64arr> segments;
	int position;
public:
	void clear() {
		segments.clear();
		position = 0;
	}
	ssize_t size() {
		if(segments.size() == 0) {
			return 0;
		}
		auto last = segments.size() - 1;
		return last * SEGMENT_SIZE + position;
	}
	void append(uint64_t u64) {
		if(segments.size() == 0 || position == SEGMENT_SIZE) {
			segments.emplace_back();
			position = 0;
		}
		auto last = segments.size() - 1;
		segments[last][position] = u64;
		position++;
	}
	uint64_t get(int i) {
		return segments[i/SEGMENT_SIZE][i%SEGMENT_SIZE];
	}
	ssize_t search(uint64_t value) {
		if(this->size() == 0 || get(0) > value) {
			return -1;
		}
		return tenary_search(value, get(0), get(size()-1), 0, size());
	}
	ssize_t tenary_search(uint64_t value, uint64_t start_value, uint64_t end_value, ssize_t start, ssize_t end) {
		while(end - start > BINSEARCH_THRES) {
			uint64_t diff_value = end_value - start_value;
			ssize_t diff_idx = end - start;
			double ratio = double(value-start_value)/double(diff_value);
			ssize_t off1 = ssize_t(ratio*0.85*double(diff_idx));
			ssize_t off2 = ssize_t(ratio*1.15*double(diff_idx));
			ssize_t mid1 = start + off1;
			ssize_t mid2 = start + off2;
			uint64_t mid1_value = get(mid1);
			uint64_t mid2_value = get(mid2);
			if((start + BINSEARCH_THRES > mid1) || 
			   (mid1 + BINSEARCH_THRES > mid2) ||
			   (mid2 + BINSEARCH_THRES > end)) {
				break;
			}
			if(value < mid1_value) {
				end_value = mid2_value;
				end = mid2;
			} else if(value == mid1_value) {
				return mid1;
			} else if(value < mid2_value) {
				start_value = mid1_value;
				start = mid1;
				end_value = mid2_value;
				end = mid2;
			} else if(value == mid2_value) {
				return mid2;
			} else {
				start_value = mid2_value;
				start = mid2;
			}
		}
		return binary_search(value, start, end-start);
	}
	ssize_t binary_search(uint64_t value, ssize_t low, ssize_t size) {
		while (size > 0) {
			ssize_t half = size / 2;
			ssize_t other_half = size - half;
			ssize_t probe = low + half;
			ssize_t other_low = low + other_half;
			size = half;
			if(get(probe) < value) {
			        low = other_low;
			}
		}
		return low;
	}
};

}

#pragma once
#include "stdint.h"
#include <array>
#include <vector>

namespace chainkv {

class u64vec {
	enum {
		segment_size = 128*1024/8,
	};
	typedef std::array<uint64_t, segment_size> u64arr;
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
		return last * segment_size + position;
	}
	void append(uint64_t u64) {
		if(segments.size() == 0 || position == segment_size) {
			segments.emplace_back();
			position = 0;
		}
		auto last = segments.size() - 1;
		segments[last][position] = u64;
		position++;
	}
	uint64_t get(int i) {
		return segments[i/segment_size][i%segment_size];
	}
	ssize_t binary_search(uint64_t value) {
		if(this->size() == 0 || get(0) > value) {
			return -1;
		}
		ssize_t size = this->size();
		ssize_t low = 0;
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

#include <stdio.h>
#include <dirent.h>
#include <algorithm>
#include <vector>
#include <iostream>

bool read_file_list(const char* dirname, std::vector<int>& file_list) {
	DIR *dp = opendir(dirname);
	if (dp == nullptr) {
		std::cerr<<"Cannot open dir: "<<dirname<<std::endl;
		return false;
	}
	bool parse_ok = true;
	for(;;) {
		struct dirent *entry = readdir(dp);
		if(entry == nullptr) break;
		if(entry->d_type != DT_REG) continue;
		int i;
		int res = sscanf(entry->d_name, "%d", &i);
		if(res != 1) {
			std::cerr<<"Cannot parse file: "<<entry->d_name<<" i "<<i<<std::endl;
			parse_ok = false;
			continue;
		}
		file_list.push_back(i);
	}

	closedir(dp);
	sort(file_list.begin(), file_list.end());
	return parse_ok;
}

int main(int argc, const char**argv) {
	std::vector<int> file_list;
	bool ok = read_file_list(argc > 1 ? argv[1] : "/", file_list);
	std::cout<<"ok "<<ok<<std::endl;
	for(int i=0; i<file_list.size(); i++) {
		std::cout<<file_list[i]<<std::endl;
	}
	return 0;
}


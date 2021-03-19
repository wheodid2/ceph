#include <iostream>
#include <map>
#include <vector>

int main() {
	int active_mds_num = 3;

	std::map<std::string,std::map<int,int>> m;
	std::map<std::string,int> mds1;
	std::map<std::string,int> mds2;
	std::map<std::string,int> mds3;
	std::vector<int> mds_id;
	std::vector<std::map<std::string,int>> map_vector;
	std::vector<std::map<std::string,double>> gvf_vector;
	std::map<std::string,int> tot_map;


	mds1.insert(std::make_pair("A", 1));
	mds1.insert(std::make_pair("B", 3));
	mds1.insert(std::make_pair("C", 5));

	mds2.insert(std::make_pair("A", 2));
	mds2.insert(std::make_pair("B", 4));

	mds3.insert(std::make_pair("B", 3));
	mds3.insert(std::make_pair("C", 6));
	mds3.insert(std::make_pair("D", 10));
	mds3.insert(std::make_pair("A", 4));

	mds_id.emplace_back(1);
	mds_id.emplace_back(2);
	mds_id.emplace_back(3);

	map_vector.emplace_back(mds1);
	map_vector.emplace_back(mds2);
	map_vector.emplace_back(mds3);
	

	//std::string str = "A";
  //if (m.find(str) == m.end()) {
	//	std::cout << "Cannot find " << str << std::endl;
	//}
	
	// 1. Make global map
  for (int i = 0; i < active_mds_num; i++) {
		for (auto it = map_vector[i].begin(); it != map_vector[i].end(); it++) {
			if (m.find(it->first) == m.end()) {
				std::map<int,int> temp_m;
	  	  temp_m.insert(std::make_pair(mds_id[i], it->second));
				auto ret = m.insert(std::make_pair(it->first, temp_m));
				if (ret.second == false)
				  std::cout << "RET False from find false" << std::endl;
			}
			else {
				auto ret = m[it->first].insert(std::make_pair(mds_id[i], it->second));
				if (ret.second == false)
				  std::cout << "RET False from find true" << std::endl;
			}
			std::cout << "VolumeId: " << it->first << ", MDS#: " << mds_id[i] << ", Count: " << it->second << std::endl;
		}
	}

	std::cout << "-----------------------------------------" << std::endl;

	for (auto it = m.begin(); it != m.end(); it++) {
		for (auto vit = it->second.begin(); vit != it->second.end(); vit++) {
		  std::cout << "VolumeId: " << it->first << ", MDS#: " << vit->first << ", Count: " << vit->second << std::endl;
		}
	}

	std::cout << "-----------------------------------------" << std::endl;

  // 2. Calculate total volume count for each volume
  for (auto it = m.begin(); it != m.end(); it++) {
		int tot = 0;
		for (auto vit = it->second.begin(); vit != it->second.end(); vit++) {
			tot +=  vit->second;
		}
		auto ret = tot_map.insert(std::make_pair(it->first, tot));
		if (ret.second == false)
		  std::cout << "tot_map insert fail" << std::endl;
	}

	for (auto it = tot_map.begin(); it != tot_map.end(); it++) {
		std::cout << "VolumeId: " << it->first << ", Total_cnt: " << it->second << std::endl;
	}

	std::cout << "-----------------------------------------" << std::endl;

	// 3. Make gvf map for each MDS
	for (int i = 0; i < active_mds_num; i++) {
		std::map<std::string,double> temp_gvf_map;
	  for (auto it = m.begin(); it != m.end(); it++) {
	  	if (it->second.find(mds_id[i]) != it->second.end()) {
				//std::cout << "MDS#: " << mds_id[i] << ", VolumeId: " << it->first << ", gvf: " << (double) it->second[mds_id[i]]/tot_map[it->first] << std::endl;
				auto ret = temp_gvf_map.insert(std::make_pair(it->first, (double) it->second[mds_id[i]]/tot_map[it->first]));
				if (ret.second == false)
				  std::cout << "temp_gvf_map insert fail" << std::endl;
			}
	  }
		gvf_vector.emplace_back(temp_gvf_map);
	}

  std::cout << "-----------------------------------------" << std::endl;

  for (int i = 0; i < active_mds_num; i++) {
		for (auto it = gvf_vector[i].begin(); it != gvf_vector[i].end(); it++) {
			std::cout << "MDS#: " << mds_id[i] << ", VolumeId: " << it->first << ", gvf: " << it->second << std::endl;
		}
	}
}
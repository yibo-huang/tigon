//
// Created by Yi Lu on 7/19/18.
//

#pragma once

#include <vector>
#include "benchmark/ycsb/Context.h"
#include "benchmark/ycsb/Random.h"
#include "common/Zipf.h"

namespace star
{
namespace ycsb
{

template <std::size_t N> struct YCSBQuery {
	int32_t Y_KEY[N];
	bool UPDATE[N];
	bool cross_partition;
	int parts[5];
	int granules[5][10];
	int part_granule_count[5];
	int num_parts = 0;

	int32_t get_part(int i)
	{
		DCHECK(i < num_parts);
		return parts[i];
	}

	int32_t get_part_granule_count(int i)
	{
		return part_granule_count[i];
	}

	int32_t get_granule(int i, int j)
	{
		DCHECK(i < num_parts);
		DCHECK(j < part_granule_count[i]);
		return granules[i][j];
	}

	int number_of_parts()
	{
		return num_parts;
	}
};

template <std::size_t N> class makeYCSBQuery {
    public:
	YCSBQuery<N> operator()(const Context &context, uint32_t partitionID, uint32_t granuleID, Random &random, const Partitioner &partitioner) const
	{
		YCSBQuery<N> query;
		query.cross_partition = false;
		query.num_parts = 1;
		query.parts[0] = partitionID;
		query.part_granule_count[0] = 0;
		int readOnly = random.uniform_dist(1, 100);
		int crossPartition = random.uniform_dist(1, 100);
		// int crossPartitionPartNum = context.crossPartitionPartNum;
		int crossPartitionPartNum = random.uniform_dist(2, context.crossPartitionPartNum);
		for (auto i = 0u; i < N; i++) {
			// read or write

			if (readOnly <= context.readOnlyTransaction) {
				query.UPDATE[i] = false;
			} else {
				int readOrWrite = random.uniform_dist(1, 100);
				if (readOrWrite <= context.readWriteRatio) {
					query.UPDATE[i] = false;
				} else {
					query.UPDATE[i] = true;
				}
			}

			uint32_t key;

			// generate a key in a partition
			bool retry;
			do {
				retry = false;

				if (context.isUniform) {
					// For the first key, we ensure that it will land in the granule specified by granuleID.
					// This granule will be served as the coordinating granule
					key = i == 0 ? random.uniform_dist(0, static_cast<uint32_t>(context.keysPerGranule) - 1) :
						       random.uniform_dist(0, static_cast<uint32_t>(context.keysPerPartition) - 1);
				} else {
					key = i == 0 ? random.uniform_dist(0, static_cast<uint32_t>(context.keysPerGranule) - 1) :
						       Zipf::globalZipf().value(random.next_double());
				}
				int this_partition_idx = 0;
				if (crossPartition <= context.crossPartitionProbability && context.partition_num > 1) {
					if (query.num_parts == 1) {
						query.num_parts = 1;
						for (int j = query.num_parts; j < crossPartitionPartNum; ++j) {
							if (query.num_parts >= (int)context.partition_num)
								break;
							int32_t pid = random.uniform_dist(0, context.partition_num - 1);
							do {
								bool good = true;
								for (int k = 0; k < j; ++k) {
									if (query.parts[k] == pid) {
										good = false;
									}
								}
								if (good == true)
									break;
								pid = random.uniform_dist(0, context.partition_num - 1);
							} while (true);
							query.parts[query.num_parts] = pid;
							query.part_granule_count[query.num_parts] = 0;
							query.num_parts++;
						}
					}
					auto newPartitionID = query.parts[i % query.num_parts];
					query.Y_KEY[i] = i == 0 ? context.getGlobalKeyID(key, newPartitionID, granuleID) :
								  context.getGlobalKeyID(key, newPartitionID);
					query.cross_partition = true;
					this_partition_idx = i % query.num_parts;
				} else {
					query.Y_KEY[i] = i == 0 ? context.getGlobalKeyID(key, partitionID, granuleID) :
								  context.getGlobalKeyID(key, partitionID);
				}

				for (auto k = 0u; k < i; k++) {
					if (query.Y_KEY[k] == query.Y_KEY[i]) {
						retry = true;
						break;
					}
				}
				if (retry == false) {
					auto granuleId = (int)context.getGranule(query.Y_KEY[i]);
					bool good = true;
					for (int32_t k = 0; k < query.part_granule_count[this_partition_idx]; ++k) {
						if (query.granules[this_partition_idx][k] == granuleId) {
							good = false;
							break;
						}
					}
					if (good == true) {
						query.granules[this_partition_idx][query.part_granule_count[this_partition_idx]++] = granuleId;
					}
				}
			} while (retry);
		}
		return query;
	}

	// YCSBQuery<N> operator()(const Context &context, uint32_t partitionID, uint32_t granuleID,
	//                         Random &random, const Partitioner & partitioner) const {
	//   YCSBQuery<N> query;
	//   int readOnly = random.uniform_dist(1, 100);
	//   int crossPartition = random.uniform_dist(1, 100);

	//   for (auto i = 0u; i < N; i++) {
	//     // read or write

	//     if (readOnly <= context.readOnlyTransaction) {
	//       query.UPDATE[i] = false;
	//     } else {
	//       int readOrWrite = random.uniform_dist(1, 100);
	//       if (readOrWrite <= context.readWriteRatio) {
	//         query.UPDATE[i] = false;
	//       } else {
	//         query.UPDATE[i] = true;
	//       }
	//     }

	//     int32_t key;

	//     // generate a key in a partition
	//     bool retry;
	//     do {
	//       retry = false;

	//       key = Zipf::globalZipf().value(random.next_double());
	//       query.Y_KEY[i] = key;
	//       // if (crossPartition <= context.crossPartitionProbability &&
	//       //     context.partition_num > 1) {
	//       //   auto newPartitionID = partitionID;
	//       //   while (newPartitionID == partitionID) {
	//       //     newPartitionID = random.uniform_dist(0, context.partition_num - 1);
	//       //   }
	//       //   query.Y_KEY[i] = context.getGlobalKeyID(key, newPartitionID);
	//       // } else {
	//       //   query.Y_KEY[i] = context.getGlobalKeyID(key, partitionID);
	//       // }

	//       for (auto k = 0u; k < i; k++) {
	//         if (query.Y_KEY[k] == query.Y_KEY[i]) {
	//           retry = true;
	//           break;
	//         }
	//       }
	//     } while (retry);
	//   }
	//   return query;
	// }
};
} // namespace ycsb
} // namespace star

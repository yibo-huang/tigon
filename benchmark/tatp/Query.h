//
// Created by Yi Lu on 7/19/18.
//

#pragma once

#include <vector>
#include "benchmark/tatp/Context.h"
#include "benchmark/tatp/Random.h"
#include "common/Zipf.h"

namespace star
{
namespace tatp
{

struct GetSubsciberDataQuery {
	uint64_t S_ID;
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

class makeGetSubsciberDataQuery {
    public:
	GetSubsciberDataQuery operator()(const Context &context, uint32_t partitionID, uint32_t granuleID, Random &random) const
	{
		GetSubsciberDataQuery query;
		query.num_parts = 1;
		query.part_granule_count[0] = 0;
		int crossPartition = random.uniform_dist(1, 100);

                // pick a partition first
                if (crossPartition <= context.crossPartitionProbability && context.partition_num > 1) {
                        int32_t pid = -1;
                        while (true) {
                                pid = random.uniform_dist(0, context.partition_num - 1);
                                if (pid != partitionID) {
                                        break;
                                }
                        }
                        query.parts[0] = pid;
                        query.cross_partition = true;
                } else {
		        query.parts[0] = partitionID;
                        query.cross_partition = false;
                }

                uint64_t subscriber_id = 0;

                // generate an subscriber ID in a partition
                subscriber_id = random.uniform_dist(0, static_cast<uint64_t>(context.numSubScriberPerPartition) - 1);

                // get the global key based on the generated partition ID
                subscriber_id = context.getGlobalAccountID(subscriber_id, query.parts[0]);

                query.S_ID = subscriber_id;

		return query;
	}
};

} // namespace tatp
} // namespace star

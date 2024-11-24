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
	uint32_t S_ID;
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

                // always pick the local partition
                query.parts[0] = partitionID;
                query.cross_partition = false;

                uint32_t subscriber_id = 0;

                // generate an subscriber ID in a partition
                if (context.isUniform) {
                        subscriber_id = random.uniform_dist(0, static_cast<uint32_t>(context.numSubScriberPerPartition) - 1);
                } else {
                        subscriber_id = Zipf::globalZipf().value(random.next_double());
                }

                // get the global key based on the generated partition ID
                subscriber_id = context.getGlobalAccountID(subscriber_id, query.parts[0]);

                query.S_ID = subscriber_id;

		return query;
	}
};

struct GetAccessDataQuery {
	uint32_t S_ID;
        uint8_t AI_TYPE;

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

class makeGetAccessDataQuery {
    public:
	GetAccessDataQuery operator()(const Context &context, uint32_t partitionID, uint32_t granuleID, Random &random) const
	{
		GetAccessDataQuery query;
		query.num_parts = 1;
		query.part_granule_count[0] = 0;
		int crossPartition = random.uniform_dist(1, 100);

                // always pick the local partition
                query.parts[0] = partitionID;
                query.cross_partition = false;

                uint32_t subscriber_id = 0;

                // generate an subscriber ID in a partition
                if (context.isUniform) {
                        subscriber_id = random.uniform_dist(0, static_cast<uint32_t>(context.numSubScriberPerPartition) - 1);
                } else {
                        subscriber_id = Zipf::globalZipf().value(random.next_double());
                }

                // get the global key based on the generated partition ID
                subscriber_id = context.getGlobalAccountID(subscriber_id, query.parts[0]);

                query.S_ID = subscriber_id;

                // generate ai_type
                query.AI_TYPE = random.uniform_dist(1, 4);

		return query;
	}
};

} // namespace tatp
} // namespace star

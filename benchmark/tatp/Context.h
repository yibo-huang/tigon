//
// Created by Yi Lu on 7/19/18.
//

#pragma once

#include "core/Context.h"

namespace star
{
namespace tatp
{

enum class TATPWorkloadType { MIXED };

class Context : public star::Context {
    public:
	TATPWorkloadType workloadType = TATPWorkloadType::MIXED;

        std::size_t getPartitionID(uint64_t subscriber_id) const
	{
		DCHECK(subscriber_id >= 0 && subscriber_id < partition_num * numSubScriberPerPartition);
                return subscriber_id / numSubScriberPerPartition;
	}

        uint64_t getGlobalAccountID(uint64_t subscriber_id, std::size_t partitionID) const
	{
		DCHECK(subscriber_id >= 0 && subscriber_id < numSubScriberPerPartition && partitionID >= 0 && partitionID < partition_num);
		uint64_t ret_key = partitionID * numSubScriberPerPartition + subscriber_id;
		DCHECK(ret_key >= 0 && ret_key < partition_num * numSubScriberPerPartition);
		return ret_key;
	}

	Context get_single_partition_context() const
	{
                CHECK(0);
	}

	Context get_cross_partition_context() const
	{
		CHECK(0);
	}

        bool isUniform = true;

        int crossPartitionProbability = 0; // out of 100
	std::size_t numSubScriberPerPartition = 250000;
};
} // namespace tatp
} // namespace star

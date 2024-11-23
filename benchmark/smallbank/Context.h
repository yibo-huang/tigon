//
// Created by Yi Lu on 7/19/18.
//

#pragma once

#include "core/Context.h"

namespace star
{
namespace smallbank
{

enum class SmallBankWorkloadType { MIXED };

class Context : public star::Context {
    public:
	SmallBankWorkloadType workloadType = SmallBankWorkloadType::MIXED;

        std::size_t getPartitionID(uint64_t account_id) const
	{
		DCHECK(account_id >= 0 && account_id < partition_num * accountsPerPartition);
                return account_id / accountsPerPartition;
	}

        uint64_t getGlobalAccountID(uint64_t account_id, std::size_t partitionID) const
	{
		DCHECK(account_id >= 0 && account_id < accountsPerPartition && partitionID >= 0 && partitionID < partition_num);
		uint64_t ret_key = partitionID * accountsPerPartition + account_id;
		DCHECK(ret_key >= 0 && ret_key < partition_num * accountsPerPartition);
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
	std::size_t accountsPerPartition = 10000000;
};
} // namespace smallbank
} // namespace star

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

	Context get_single_partition_context() const
	{
                CHECK(0);
	}

	Context get_cross_partition_context() const
	{
		CHECK(0);
	}

        int crossPartitionProbability = 0; // out of 100
	std::size_t accountsPerPartition = 10000000;
};
} // namespace smallbank
} // namespace star

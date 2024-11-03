//
// Created by Yi Lu on 7/19/18.
//

#pragma once

#include "core/Context.h"

#include <glog/logging.h>

namespace star
{
namespace ycsb
{

enum class YCSBWorkloadType { RMW, SCAN, INSERT, DELETE, MIXED };

enum class PartitionStrategy { RANGE, ROUND_ROBIN };

class Context : public star::Context {
    public:
        YCSBWorkloadType workloadType = YCSBWorkloadType::RMW;

	std::size_t getPartitionID(std::size_t key) const
	{
		DCHECK(key >= 0 && key < partition_num * keysPerPartition);

		if (strategy == PartitionStrategy::ROUND_ROBIN) {
			return key % partition_num;
		} else {
			return key / keysPerPartition;
		}
	}
	static bool tested;
	static void unit_testing(Context *ctx)
	{
		for (std::size_t i = 0; i < ctx->partition_num; ++i) {
			for (std::size_t k = 0; k < ctx->keysPerGranule; ++k) {
				auto complete_key = ctx->getGlobalKeyID(k, i);
				CHECK(ctx->getPartitionID(complete_key) == i);
			}
		}
	}

	std::size_t getGranule(std::size_t key) const
	{
		DCHECK(key >= 0 && key < partition_num * keysPerPartition);
		CHECK(granules_per_partition > 0);

		if (strategy == PartitionStrategy::ROUND_ROBIN) {
			auto partitionID = getPartitionID(key);
			return (key - partitionID) / partition_num % granules_per_partition;
		} else {
			return key % keysPerPartition % granules_per_partition;
		}
	}

	std::size_t getGlobalKeyID(std::size_t key, std::size_t partitionID, std::size_t granuleID) const
	{
		DCHECK(key >= 0 && key < keysPerGranule && partitionID >= 0 && partitionID < partition_num && granuleID >= 0 &&
		       granuleID < granules_per_partition);
		std::size_t ret_key;
		if (strategy == PartitionStrategy::ROUND_ROBIN) {
			ret_key = (key * granules_per_partition + granuleID) * partition_num + partitionID;
		} else {
			ret_key = partitionID * keysPerPartition + granuleID * keysPerGranule + key;
		}
		CHECK(ret_key >= 0 && ret_key < partition_num * keysPerPartition);
		return ret_key;
	}

	std::size_t getGlobalKeyID(std::size_t key, std::size_t partitionID) const
	{
		DCHECK(key >= 0 && key < keysPerPartition && partitionID >= 0 && partitionID < partition_num);
		std::size_t ret_key;
		if (strategy == PartitionStrategy::ROUND_ROBIN) {
			ret_key = key * partition_num + partitionID;
		} else {
			ret_key = partitionID * keysPerPartition + key;
		}
		CHECK(ret_key >= 0 && ret_key < partition_num * keysPerPartition);
		return ret_key;
	}

	Context get_single_partition_context() const
	{
		Context c = *this;
		c.crossPartitionProbability = 0;
		c.operation_replication = this->operation_replication;
		c.star_sync_in_single_master_phase = false;
		return c;
	}

	Context get_cross_partition_context() const
	{
		Context c = *this;
		c.crossPartitionProbability = 100;
		c.operation_replication = false;
		c.star_sync_in_single_master_phase = this->star_sync_in_single_master_phase;
		return c;
	}

    public:
	int readWriteRatio = 0; // out of 100
	int readOnlyTransaction = 0; //  out of 100
	int crossPartitionProbability = 0; // out of 100
	int crossPartitionPartNum = 2;
	std::size_t keysPerTransaction = 10;
	std::size_t keysPerPartition = 200000;
	std::size_t keysPerGranule = 2000;

	std::size_t nop_prob = 0; // out of 10000
	std::size_t n_nop = 0;

	bool isUniform = true;

	PartitionStrategy strategy = PartitionStrategy::RANGE;
};
bool Context::tested = false;
} // namespace ycsb
} // namespace star

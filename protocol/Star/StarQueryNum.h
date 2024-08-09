//
// Created by Yi Lu on 9/21/18.
//

#pragma once

#include "benchmark/tpcc/Context.h"
#include "benchmark/ycsb/Context.h"

namespace star
{
template <class Context> class StarQueryNum {
    public:
	static std::size_t get_s_phase_query_num(const Context &context, uint32_t batch_size)
	{
		CHECK(false) << "not supported.";
		return 0;
	}

	static std::size_t get_c_phase_query_num(const Context &context, uint32_t batch_size)
	{
		CHECK(false) << "not supported.";
		return 0;
	}
};

template <> class StarQueryNum<star::tpcc::Context> {
    public:
	static std::size_t get_s_phase_query_num(const star::tpcc::Context &context, uint32_t batch_size)
	{
		if (context.workloadType == star::tpcc::TPCCWorkloadType::NEW_ORDER_ONLY) {
			return batch_size * (100 - context.newOrderCrossPartitionProbability) / 100;
		} else if (context.workloadType == star::tpcc::TPCCWorkloadType::PAYMENT_ONLY) {
			return batch_size * (100 - context.paymentCrossPartitionProbability) / 100;
		} else {
			return (batch_size * (100 - context.newOrderCrossPartitionProbability) / 100 +
				batch_size * (100 - context.paymentCrossPartitionProbability) / 100) /
			       2;
		}
	}

	static std::size_t get_c_phase_query_num(const star::tpcc::Context &context, uint32_t batch_size)
	{
		if (context.workloadType == star::tpcc::TPCCWorkloadType::NEW_ORDER_ONLY) {
			return context.coordinator_num * batch_size * context.newOrderCrossPartitionProbability / 100;
		} else if (context.workloadType == star::tpcc::TPCCWorkloadType::PAYMENT_ONLY) {
			return context.coordinator_num * batch_size * context.paymentCrossPartitionProbability / 100;
		} else {
			return context.coordinator_num *
			       (batch_size * context.newOrderCrossPartitionProbability / 100 + batch_size * context.paymentCrossPartitionProbability / 100) / 2;
		}
	}
};

template <> class StarQueryNum<star::ycsb::Context> {
    public:
	static std::size_t get_s_phase_query_num(const star::ycsb::Context &context, uint32_t batch_size)
	{
		return batch_size * (100 - context.crossPartitionProbability) / 100.0;
	}

	static std::size_t get_c_phase_query_num(const star::ycsb::Context &context, uint32_t batch_size)
	{
		return context.coordinator_num * batch_size * context.crossPartitionProbability / 100.0;
	}
};
} // namespace star

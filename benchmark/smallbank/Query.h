//
// Created by Yi Lu on 7/19/18.
//

#pragma once

#include <vector>
#include "benchmark/smallbank/Context.h"
#include "benchmark/smallbank/Random.h"
#include "common/Zipf.h"

namespace star
{
namespace smallbank
{

struct BalanceQuery {
	uint64_t account_id;
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

class makeBalanceQuery {
    public:
	BalanceQuery operator()(const Context &context, uint32_t partitionID, uint32_t granuleID, Random &random) const
	{
		BalanceQuery query;
		query.num_parts = 1;
		query.part_granule_count[0] = 0;
		int crossPartition = random.uniform_dist(1, 100);

                // always pick the local partition
                query.parts[0] = partitionID;
                query.cross_partition = false;

                uint64_t account_id = 0;

                // generate an account ID in a partition
                if (context.isUniform) {
                        account_id = random.uniform_dist(0, static_cast<uint64_t>(context.accountsPerPartition) - 1);
                } else {
                        account_id = Zipf::globalZipf().value(random.next_double());
                }

                // get the global key based on the generated partition ID
                account_id = context.getGlobalAccountID(account_id, query.parts[0]);

                query.account_id = account_id;

		return query;
	}
};

struct DepositCheckingQuery {
	uint64_t account_id;
        float amount;
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

class makeDepositCheckingQuery {
    public:
	DepositCheckingQuery operator()(const Context &context, uint32_t partitionID, uint32_t granuleID, Random &random) const
	{
		DepositCheckingQuery query;
		query.num_parts = 1;
		query.part_granule_count[0] = 0;
		int crossPartition = random.uniform_dist(1, 100);

                // always pick the local partition
                query.parts[0] = partitionID;
                query.cross_partition = false;

                uint64_t account_id = 0;

                // generate an account ID in a partition
                if (context.isUniform) {
                        account_id = random.uniform_dist(0, static_cast<uint64_t>(context.accountsPerPartition) - 1);
                } else {
                        account_id = Zipf::globalZipf().value(random.next_double());
                }

                // get the global key based on the generated partition ID
                account_id = context.getGlobalAccountID(account_id, query.parts[0]);

                query.account_id = account_id;

                // like Motor, 1.3
                query.amount = 1.3;

		return query;
	}
};

struct TransactSavingQuery {
	uint64_t account_id;
        float amount;   // can be negative
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

class makeTransactSavingQuery {
    public:
	TransactSavingQuery operator()(const Context &context, uint32_t partitionID, uint32_t granuleID, Random &random) const
	{
		TransactSavingQuery query;
		query.num_parts = 1;
		query.part_granule_count[0] = 0;
		int crossPartition = random.uniform_dist(1, 100);

                // always pick the local partition
                query.parts[0] = partitionID;
                query.cross_partition = false;

                uint64_t account_id = 0;

                // generate an account ID in a partition
                if (context.isUniform) {
                        account_id = random.uniform_dist(0, static_cast<uint64_t>(context.accountsPerPartition) - 1);
                } else {
                        account_id = Zipf::globalZipf().value(random.next_double());
                }

                // get the global key based on the generated partition ID
                account_id = context.getGlobalAccountID(account_id, query.parts[0]);

                query.account_id = account_id;

                // like Motor, 20.20
                query.amount = 20.20;

		return query;
	}
};

struct AmalgamateQuery {
	uint64_t first_account_id;
        uint64_t second_account_id;
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

class makeAmalgamateQuery {
    public:
	AmalgamateQuery operator()(const Context &context, uint32_t partitionID, uint32_t granuleID, Random &random) const
	{
		AmalgamateQuery query;
		query.part_granule_count[0] = 0;
		int crossPartition = random.uniform_dist(1, 100);

                // the first account is always chosen from local partition
                query.parts[0] = partitionID;

                // pick the second partition
                if (crossPartition <= context.crossPartitionProbability && context.partition_num > 1) {
                        // choose a different partition
                        int32_t pid = -1;
                        while (true) {
                                pid = random.uniform_dist(0, context.partition_num - 1);
                                if (pid != partitionID) {
                                        break;
                                }
                        }
                        query.parts[1] = pid;
                        query.cross_partition = true;
                        query.num_parts = 2;
                } else {
                        // choose local partition
		        query.parts[1] = partitionID;
                        query.cross_partition = false;
                        query.num_parts = 1;
                }

                uint64_t first_account_id = 0, second_account_id = 0;

                // generate two distinct account IDs
                if (context.isUniform) {
                        first_account_id = random.uniform_dist(0, static_cast<uint64_t>(context.accountsPerPartition) - 1);
                        first_account_id = context.getGlobalAccountID(first_account_id, query.parts[0]);
                        while (true) {
                                second_account_id = random.uniform_dist(0, static_cast<uint64_t>(context.accountsPerPartition) - 1);
                                second_account_id = context.getGlobalAccountID(second_account_id, query.parts[1]);
                                if (second_account_id != first_account_id) {
                                        break;
                                }
                        }
                } else {
                        first_account_id = Zipf::globalZipf().value(random.next_double());
                        first_account_id = context.getGlobalAccountID(first_account_id, query.parts[0]);
                        while (true) {
                                second_account_id = Zipf::globalZipf().value(random.next_double());
                                second_account_id = context.getGlobalAccountID(second_account_id, query.parts[1]);
                                if (second_account_id != first_account_id) {
                                        break;
                                }
                        }
                }

                query.first_account_id = first_account_id;
                query.second_account_id = second_account_id;

		return query;
	}
};

struct WriteCheckQuery {
	uint64_t account_id;
        float amount;   // can be negative
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

class makeWriteCheckQuery {
    public:
	WriteCheckQuery operator()(const Context &context, uint32_t partitionID, uint32_t granuleID, Random &random) const
	{
		WriteCheckQuery query;
		query.num_parts = 1;
		query.part_granule_count[0] = 0;
		int crossPartition = random.uniform_dist(1, 100);

                // always pick the local partition
                query.parts[0] = partitionID;
                query.cross_partition = false;

                uint64_t account_id = 0;

                // generate an account ID in a partition
                if (context.isUniform) {
                        account_id = random.uniform_dist(0, static_cast<uint64_t>(context.accountsPerPartition) - 1);
                } else {
                        account_id = Zipf::globalZipf().value(random.next_double());
                }

                // get the global key based on the generated partition ID
                account_id = context.getGlobalAccountID(account_id, query.parts[0]);

                query.account_id = account_id;

                // like Motor, 5.0
                query.amount = 5.0;

		return query;
	}
};

struct SendPaymentQuery {
	uint64_t first_account_id;
        uint64_t second_account_id;
        float amount;
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

class makeSendPaymentQuery {
    public:
	SendPaymentQuery operator()(const Context &context, uint32_t partitionID, uint32_t granuleID, Random &random) const
	{
		SendPaymentQuery query;
		query.part_granule_count[0] = 0;
		int crossPartition = random.uniform_dist(1, 100);

                // the first account is always chosen from local partition
                query.parts[0] = partitionID;

                // pick the second partition
                if (crossPartition <= context.crossPartitionProbability && context.partition_num > 1) {
                        // choose a different partition
                        int32_t pid = -1;
                        while (true) {
                                pid = random.uniform_dist(0, context.partition_num - 1);
                                if (pid != partitionID) {
                                        break;
                                }
                        }
                        query.parts[1] = pid;
                        query.cross_partition = true;
                        query.num_parts = 2;
                } else {
                        // choose local partition
		        query.parts[1] = partitionID;
                        query.cross_partition = false;
                        query.num_parts = 1;
                }

                uint64_t first_account_id = 0, second_account_id = 0;

                // generate two distinct account IDs
                if (context.isUniform) {
                        first_account_id = random.uniform_dist(0, static_cast<uint64_t>(context.accountsPerPartition) - 1);
                        first_account_id = context.getGlobalAccountID(first_account_id, query.parts[0]);
                        while (true) {
                                second_account_id = random.uniform_dist(0, static_cast<uint64_t>(context.accountsPerPartition) - 1);
                                second_account_id = context.getGlobalAccountID(second_account_id, query.parts[1]);
                                if (second_account_id != first_account_id) {
                                        break;
                                }
                        }
                } else {
                        first_account_id = Zipf::globalZipf().value(random.next_double());
                        first_account_id = context.getGlobalAccountID(first_account_id, query.parts[0]);
                        while (true) {
                                second_account_id = Zipf::globalZipf().value(random.next_double());
                                second_account_id = context.getGlobalAccountID(second_account_id, query.parts[1]);
                                if (second_account_id != first_account_id) {
                                        break;
                                }
                        }
                }

                query.first_account_id = first_account_id;
                query.second_account_id = second_account_id;

                // like Motor, 5.0
                query.amount = 5.0;

		return query;
	}
};

} // namespace ycsb
} // namespace star

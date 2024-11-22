//
// Created by Yi Lu on 7/22/18.
//

#pragma once

#include "glog/logging.h"

#include "benchmark/smallbank/Database.h"
#include "benchmark/smallbank/Query.h"
#include "benchmark/smallbank/Schema.h"
#include "benchmark/smallbank/Storage.h"
#include "common/Operation.h"
#include "core/Defs.h"
#include "core/Partitioner.h"
#include "core/Table.h"

namespace star
{
namespace smallbank
{
static thread_local std::vector<Storage *> storage_cache;

Storage *get_storage()
{
	if (storage_cache.empty()) {
		for (size_t i = 0; i < 10; ++i) {
			storage_cache.push_back(new Storage());
		}
	}
	Storage *last = storage_cache.back();
	storage_cache.pop_back();
	return last;
}

void put_storage(Storage *s)
{
	storage_cache.push_back(s);
}

template <class Transaction> class Balance : public Transaction {
    public:
	using DatabaseType = Database;
	using ContextType = typename DatabaseType::ContextType;
	using RandomType = typename DatabaseType::RandomType;
	using StorageType = Storage;

	Balance(std::size_t coordinator_id, std::size_t partition_id, std::size_t granule_id, DatabaseType &db, const ContextType &context,
			RandomType &random, Partitioner &partitioner, std::size_t ith_replica = 0)
		: Transaction(coordinator_id, partition_id, partitioner, ith_replica)
		, db(db)
		, context(context)
		, random(random)
		, partition_id(partition_id)
		, granule_id(granule_id)
		, query(makeBalanceQuery()(context, partition_id, granule_id, random))
	{
		storage = get_storage();
	}

        virtual ~Balance()
	{
		put_storage(storage);
		storage = nullptr;
	}

	virtual int32_t get_partition_count() override
	{
		return query.number_of_parts();
	}

	virtual int32_t get_partition(int ith_partition) override
	{
		return query.get_part(ith_partition);
	}

	virtual int32_t get_partition_granule_count(int ith_partition) override
	{
		return query.get_part_granule_count(ith_partition);
	}

	virtual int32_t get_granule(int ith_partition, int j) override
	{
		return query.get_granule(ith_partition, j);
	}

	virtual bool is_single_partition() override
	{
		return query.cross_partition == false;
	}

	virtual const std::string serialize(std::size_t ith_replica = 0) override
	{
		std::string res;
		Encoder encoder(res);
		encoder << this->transaction_id << this->straggler_wait_time << ith_replica << this->txn_random_seed_start << partition_id << granule_id;
		encoder << get_partition_count();
		// int granules_count = 0;
		// for (int32_t i = 0; i < get_partition_count(); ++i)
		//   granules_count += get_partition_granule_count(i);
		// for (int32_t i = 0; i < get_partition_count(); ++i)
		//   encoder << get_partition(i);
		// encoder << granules_count;
		// for (int32_t i = 0; i < get_partition_count(); ++i)
		//   for (int32_t j = 0; j < get_partition_granule_count(i); ++j)
		//     encoder << get_granule(i, j);
		Transaction::serialize_lock_status(encoder);
		return res;
	}

	TransactionResult execute(std::size_t worker_id) override
	{
                storage->cleanup();
		ScopedTimer t_local_work([&, this](uint64_t us) { this->record_local_work_time(us); });

                // Balance, or Bal(N), is a parameterized transaction that represents calculating the total balance for a customer.
                // It looks up Account to get the CustomerID value for N, and then returns the sum of savings and checking balances for that CustomerID.

                uint64_t account_id = query.account_id;

                CHECK(context.getPartitionID(account_id) == query.get_part(0));

                int savingsTableID = savings::tableID;
                storage->savings_key.ACCOUNT_ID = account_id;
                this->search_for_read(savingsTableID, context.getPartitionID(account_id), storage->savings_key, storage->savings_value, 0);

                int checkingTableID = checking::tableID;
                storage->first_checking_key.ACCOUNT_ID = account_id;
                this->search_for_read(checkingTableID, context.getPartitionID(account_id), storage->first_checking_key, storage->first_checking_value, 0);

		t_local_work.end();
		if (this->process_requests(worker_id)) {
			return TransactionResult::ABORT;
		}
		t_local_work.reset();

                auto sum = storage->savings_value.BALANCE + storage->first_checking_value.BALANCE;

		return TransactionResult::READY_TO_COMMIT;
	}

	void reset_query() override
	{
		query = makeBalanceQuery()(context, partition_id, granule_id, random);
	}

    private:
	DatabaseType &db;
	const ContextType &context;
	RandomType random;
	Storage *storage = nullptr;
	std::size_t partition_id, granule_id;
	BalanceQuery query;
};


template <class Transaction> class DepositChecking : public Transaction {
    public:
	using DatabaseType = Database;
	using ContextType = typename DatabaseType::ContextType;
	using RandomType = typename DatabaseType::RandomType;
	using StorageType = Storage;

	DepositChecking(std::size_t coordinator_id, std::size_t partition_id, std::size_t granule_id, DatabaseType &db, const ContextType &context,
			RandomType &random, Partitioner &partitioner, std::size_t ith_replica = 0)
		: Transaction(coordinator_id, partition_id, partitioner, ith_replica)
		, db(db)
		, context(context)
		, random(random)
		, partition_id(partition_id)
		, granule_id(granule_id)
		, query(makeDepositCheckingQuery()(context, partition_id, granule_id, random))
	{
		storage = get_storage();
	}

        virtual ~DepositChecking()
	{
		put_storage(storage);
		storage = nullptr;
	}

	virtual int32_t get_partition_count() override
	{
		return query.number_of_parts();
	}

	virtual int32_t get_partition(int ith_partition) override
	{
		return query.get_part(ith_partition);
	}

	virtual int32_t get_partition_granule_count(int ith_partition) override
	{
		return query.get_part_granule_count(ith_partition);
	}

	virtual int32_t get_granule(int ith_partition, int j) override
	{
		return query.get_granule(ith_partition, j);
	}

	virtual bool is_single_partition() override
	{
		return query.cross_partition == false;
	}

	virtual const std::string serialize(std::size_t ith_replica = 0) override
	{
		std::string res;
		Encoder encoder(res);
		encoder << this->transaction_id << this->straggler_wait_time << ith_replica << this->txn_random_seed_start << partition_id << granule_id;
		encoder << get_partition_count();
		// int granules_count = 0;
		// for (int32_t i = 0; i < get_partition_count(); ++i)
		//   granules_count += get_partition_granule_count(i);
		// for (int32_t i = 0; i < get_partition_count(); ++i)
		//   encoder << get_partition(i);
		// encoder << granules_count;
		// for (int32_t i = 0; i < get_partition_count(); ++i)
		//   for (int32_t j = 0; j < get_partition_granule_count(i); ++j)
		//     encoder << get_granule(i, j);
		Transaction::serialize_lock_status(encoder);
		return res;
	}

	TransactionResult execute(std::size_t worker_id) override
	{
                storage->cleanup();
		ScopedTimer t_local_work([&, this](uint64_t us) { this->record_local_work_time(us); });

                // DepositChecking, or DC(N,V), is a parameterized transaction that represents making a deposit on the checking account of a customer.
                // Its operation is to look up the Account table to get CustomerID corresponding to the name N and increase the checking balance by V for that CustomerID.
                // If the value V is negative or if the name N is not found in the table, the transaction will rollback.

                uint64_t account_id = query.account_id;

                CHECK(context.getPartitionID(account_id) == query.get_part(0));

                if (query.amount < 0) {
                        return TransactionResult::ABORT_NORETRY;
                }

                int checkingTableID = checking::tableID;
                storage->first_checking_key.ACCOUNT_ID = account_id;
                this->search_for_update(checkingTableID, context.getPartitionID(account_id), storage->first_checking_key, storage->first_checking_value, 0);
                this->update(checkingTableID, context.getPartitionID(account_id), storage->first_checking_key, storage->first_checking_value, 0);

		t_local_work.end();
		if (this->process_requests(worker_id)) {
			return TransactionResult::ABORT;
		}
		t_local_work.reset();

                storage->first_checking_value.BALANCE += query.amount;

		return TransactionResult::READY_TO_COMMIT;
	}

	void reset_query() override
	{
		query = makeDepositCheckingQuery()(context, partition_id, granule_id, random);
	}

    private:
	DatabaseType &db;
	const ContextType &context;
	RandomType random;
	Storage *storage = nullptr;
	std::size_t partition_id, granule_id;
	DepositCheckingQuery query;
};

template <class Transaction> class TransactSaving : public Transaction {
    public:
	using DatabaseType = Database;
	using ContextType = typename DatabaseType::ContextType;
	using RandomType = typename DatabaseType::RandomType;
	using StorageType = Storage;

	TransactSaving(std::size_t coordinator_id, std::size_t partition_id, std::size_t granule_id, DatabaseType &db, const ContextType &context,
			RandomType &random, Partitioner &partitioner, std::size_t ith_replica = 0)
		: Transaction(coordinator_id, partition_id, partitioner, ith_replica)
		, db(db)
		, context(context)
		, random(random)
		, partition_id(partition_id)
		, granule_id(granule_id)
		, query(makeTransactSavingQuery()(context, partition_id, granule_id, random))
	{
		storage = get_storage();
	}

        virtual ~TransactSaving()
	{
		put_storage(storage);
		storage = nullptr;
	}

	virtual int32_t get_partition_count() override
	{
		return query.number_of_parts();
	}

	virtual int32_t get_partition(int ith_partition) override
	{
		return query.get_part(ith_partition);
	}

	virtual int32_t get_partition_granule_count(int ith_partition) override
	{
		return query.get_part_granule_count(ith_partition);
	}

	virtual int32_t get_granule(int ith_partition, int j) override
	{
		return query.get_granule(ith_partition, j);
	}

	virtual bool is_single_partition() override
	{
		return query.cross_partition == false;
	}

	virtual const std::string serialize(std::size_t ith_replica = 0) override
	{
		std::string res;
		Encoder encoder(res);
		encoder << this->transaction_id << this->straggler_wait_time << ith_replica << this->txn_random_seed_start << partition_id << granule_id;
		encoder << get_partition_count();
		// int granules_count = 0;
		// for (int32_t i = 0; i < get_partition_count(); ++i)
		//   granules_count += get_partition_granule_count(i);
		// for (int32_t i = 0; i < get_partition_count(); ++i)
		//   encoder << get_partition(i);
		// encoder << granules_count;
		// for (int32_t i = 0; i < get_partition_count(); ++i)
		//   for (int32_t j = 0; j < get_partition_granule_count(i); ++j)
		//     encoder << get_granule(i, j);
		Transaction::serialize_lock_status(encoder);
		return res;
	}

	TransactionResult execute(std::size_t worker_id) override
	{
                storage->cleanup();
		ScopedTimer t_local_work([&, this](uint64_t us) { this->record_local_work_time(us); });

                // TransactSaving, or TS(N, V), represents making a deposit or withdrawal on the savings account. It increases the savings balance by V for that customer.
                // If the name N is not found in the table or if the transaction would result in a negative savings balance for the customer, the transaction will rollback.

                uint64_t account_id = query.account_id;

                CHECK(context.getPartitionID(account_id) == query.get_part(0));

                int savingsTableID = savings::tableID;
                storage->savings_key.ACCOUNT_ID = account_id;
                this->search_for_update(savingsTableID, context.getPartitionID(account_id), storage->savings_key, storage->savings_value, 0);
                this->update(savingsTableID, context.getPartitionID(account_id), storage->savings_key, storage->savings_value, 0);

		t_local_work.end();
		if (this->process_requests(worker_id)) {
			return TransactionResult::ABORT;
		}
		t_local_work.reset();

                storage->savings_value.BALANCE += query.amount;

                if (storage->savings_value.BALANCE < 0) {
                        return TransactionResult::ABORT_NORETRY;
                }

		return TransactionResult::READY_TO_COMMIT;
	}

	void reset_query() override
	{
		query = makeTransactSavingQuery()(context, partition_id, granule_id, random);
	}

    private:
	DatabaseType &db;
	const ContextType &context;
	RandomType random;
	Storage *storage = nullptr;
	std::size_t partition_id, granule_id;
	TransactSavingQuery query;
};

template <class Transaction> class Amalgamate : public Transaction {
    public:
	using DatabaseType = Database;
	using ContextType = typename DatabaseType::ContextType;
	using RandomType = typename DatabaseType::RandomType;
	using StorageType = Storage;

	Amalgamate(std::size_t coordinator_id, std::size_t partition_id, std::size_t granule_id, DatabaseType &db, const ContextType &context,
			RandomType &random, Partitioner &partitioner, std::size_t ith_replica = 0)
		: Transaction(coordinator_id, partition_id, partitioner, ith_replica)
		, db(db)
		, context(context)
		, random(random)
		, partition_id(partition_id)
		, granule_id(granule_id)
		, query(makeAmalgamateQuery()(context, partition_id, granule_id, random))
	{
		storage = get_storage();
	}

        virtual ~Amalgamate()
	{
		put_storage(storage);
		storage = nullptr;
	}

	virtual int32_t get_partition_count() override
	{
		return query.number_of_parts();
	}

	virtual int32_t get_partition(int ith_partition) override
	{
		return query.get_part(ith_partition);
	}

	virtual int32_t get_partition_granule_count(int ith_partition) override
	{
		return query.get_part_granule_count(ith_partition);
	}

	virtual int32_t get_granule(int ith_partition, int j) override
	{
		return query.get_granule(ith_partition, j);
	}

	virtual bool is_single_partition() override
	{
		return query.cross_partition == false;
	}

	virtual const std::string serialize(std::size_t ith_replica = 0) override
	{
		std::string res;
		Encoder encoder(res);
		encoder << this->transaction_id << this->straggler_wait_time << ith_replica << this->txn_random_seed_start << partition_id << granule_id;
		encoder << get_partition_count();
		// int granules_count = 0;
		// for (int32_t i = 0; i < get_partition_count(); ++i)
		//   granules_count += get_partition_granule_count(i);
		// for (int32_t i = 0; i < get_partition_count(); ++i)
		//   encoder << get_partition(i);
		// encoder << granules_count;
		// for (int32_t i = 0; i < get_partition_count(); ++i)
		//   for (int32_t j = 0; j < get_partition_granule_count(i); ++j)
		//     encoder << get_granule(i, j);
		Transaction::serialize_lock_status(encoder);
		return res;
	}

	TransactionResult execute(std::size_t worker_id) override
	{
                storage->cleanup();
		ScopedTimer t_local_work([&, this](uint64_t us) { this->record_local_work_time(us); });

                // Amalgamate, or Amg(NI, N2), represents moving all the funds from one customer to another.
                // It reads the balances for both accounts of customer Ni, then sets both to zero,
                // and finally increases the checking balance for N2 by the sum of Ni's previous balances.

                uint64_t first_account_id = query.first_account_id;
                uint64_t second_account_id = query.second_account_id;

                CHECK(context.getPartitionID(first_account_id) == query.get_part(0));
                CHECK(context.getPartitionID(second_account_id) == query.get_part(0));

                int checkingTableID = checking::tableID;
                storage->first_checking_key.ACCOUNT_ID = first_account_id;
                this->search_for_update(checkingTableID, context.getPartitionID(first_account_id), storage->first_checking_key, storage->first_checking_value, 0);
                this->update(checkingTableID, context.getPartitionID(first_account_id), storage->first_checking_key, storage->first_checking_value, 0);

                storage->second_checking_key.ACCOUNT_ID = second_account_id;
                this->search_for_update(checkingTableID, context.getPartitionID(second_account_id), storage->second_checking_key, storage->second_checking_value, 0);
                this->update(checkingTableID, context.getPartitionID(second_account_id), storage->second_checking_key, storage->second_checking_value, 0);

                int savingsTableID = savings::tableID;
                storage->savings_key.ACCOUNT_ID = first_account_id;
                this->search_for_update(savingsTableID, context.getPartitionID(first_account_id), storage->savings_key, storage->savings_value, 0);
                this->update(savingsTableID, context.getPartitionID(first_account_id), storage->savings_key, storage->savings_value, 0);

		t_local_work.end();
		if (this->process_requests(worker_id)) {
			return TransactionResult::ABORT;
		}
		t_local_work.reset();

                storage->second_checking_value.BALANCE += storage->first_checking_value.BALANCE;
                storage->second_checking_value.BALANCE += storage->savings_value.BALANCE;

                storage->first_checking_value.BALANCE = 0;
                storage->savings_value.BALANCE = 0;

		return TransactionResult::READY_TO_COMMIT;
	}

	void reset_query() override
	{
		query = makeAmalgamateQuery()(context, partition_id, granule_id, random);
	}

    private:
	DatabaseType &db;
	const ContextType &context;
	RandomType random;
	Storage *storage = nullptr;
	std::size_t partition_id, granule_id;
	AmalgamateQuery query;
};

template <class Transaction> class WriteCheck : public Transaction {
    public:
	using DatabaseType = Database;
	using ContextType = typename DatabaseType::ContextType;
	using RandomType = typename DatabaseType::RandomType;
	using StorageType = Storage;

	WriteCheck(std::size_t coordinator_id, std::size_t partition_id, std::size_t granule_id, DatabaseType &db, const ContextType &context,
			RandomType &random, Partitioner &partitioner, std::size_t ith_replica = 0)
		: Transaction(coordinator_id, partition_id, partitioner, ith_replica)
		, db(db)
		, context(context)
		, random(random)
		, partition_id(partition_id)
		, granule_id(granule_id)
		, query(makeWriteCheckQuery()(context, partition_id, granule_id, random))
	{
		storage = get_storage();
	}

        virtual ~WriteCheck()
	{
		put_storage(storage);
		storage = nullptr;
	}

	virtual int32_t get_partition_count() override
	{
		return query.number_of_parts();
	}

	virtual int32_t get_partition(int ith_partition) override
	{
		return query.get_part(ith_partition);
	}

	virtual int32_t get_partition_granule_count(int ith_partition) override
	{
		return query.get_part_granule_count(ith_partition);
	}

	virtual int32_t get_granule(int ith_partition, int j) override
	{
		return query.get_granule(ith_partition, j);
	}

	virtual bool is_single_partition() override
	{
		return query.cross_partition == false;
	}

	virtual const std::string serialize(std::size_t ith_replica = 0) override
	{
		std::string res;
		Encoder encoder(res);
		encoder << this->transaction_id << this->straggler_wait_time << ith_replica << this->txn_random_seed_start << partition_id << granule_id;
		encoder << get_partition_count();
		// int granules_count = 0;
		// for (int32_t i = 0; i < get_partition_count(); ++i)
		//   granules_count += get_partition_granule_count(i);
		// for (int32_t i = 0; i < get_partition_count(); ++i)
		//   encoder << get_partition(i);
		// encoder << granules_count;
		// for (int32_t i = 0; i < get_partition_count(); ++i)
		//   for (int32_t j = 0; j < get_partition_granule_count(i); ++j)
		//     encoder << get_granule(i, j);
		Transaction::serialize_lock_status(encoder);
		return res;
	}

	TransactionResult execute(std::size_t worker_id) override
	{
                storage->cleanup();
		ScopedTimer t_local_work([&, this](uint64_t us) { this->record_local_work_time(us); });

                // WriteCheck, or WC(N,V), represents writing a check against an account.
                // Its operation is to look up Account to get the CustomerlD value for N, evaluate the sum of savings and checking balances for that CustomerID.
                // If the sum is less than V, it decreases the checking balance by V+1 (reflecting a penalty of 1 for overdrawing), otherwise it decreases the checking balance by V.

                uint64_t account_id = query.account_id;

                CHECK(context.getPartitionID(account_id) == query.get_part(0));

                int checkingTableID = checking::tableID;
                storage->first_checking_key.ACCOUNT_ID = account_id;
                this->search_for_update(checkingTableID, context.getPartitionID(account_id), storage->first_checking_key, storage->first_checking_value, 0);
                this->update(checkingTableID, context.getPartitionID(account_id), storage->first_checking_key, storage->first_checking_value, 0);

                int savingsTableID = savings::tableID;
                storage->savings_key.ACCOUNT_ID = account_id;
                this->search_for_update(savingsTableID, context.getPartitionID(account_id), storage->savings_key, storage->savings_value, 0);
                this->update(savingsTableID, context.getPartitionID(account_id), storage->savings_key, storage->savings_value, 0);

		t_local_work.end();
		if (this->process_requests(worker_id)) {
			return TransactionResult::ABORT;
		}
		t_local_work.reset();

                if ((storage->first_checking_value.BALANCE + storage->savings_value.BALANCE) < query.amount) {
                        storage->first_checking_value.BALANCE -= (query.amount + 1);
                } else {
                        storage->first_checking_value.BALANCE -= query.amount;
                }

		return TransactionResult::READY_TO_COMMIT;
	}

	void reset_query() override
	{
		query = makeWriteCheckQuery()(context, partition_id, granule_id, random);
	}

    private:
	DatabaseType &db;
	const ContextType &context;
	RandomType random;
	Storage *storage = nullptr;
	std::size_t partition_id, granule_id;
	WriteCheckQuery query;
};

} // namespace smallbank

} // namespace star

//
// Created by Yi Lu on 7/22/18.
//

#pragma once

#include <unordered_set>

#include "glog/logging.h"

#include "benchmark/tpcc/Database.h"
#include "benchmark/tpcc/Query.h"
#include "benchmark/tpcc/Schema.h"
#include "benchmark/tpcc/Storage.h"
#include "common/Operation.h"
#include "common/Time.h"
#include "core/Defs.h"
#include "core/Partitioner.h"

#include "core/Table.h"

namespace star
{
namespace tpcc
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

template <class Transaction> class NewOrder : public Transaction {
    public:
	using DatabaseType = Database;
	using ContextType = typename DatabaseType::ContextType;
	using RandomType = typename DatabaseType::RandomType;
	using StorageType = Storage;

	NewOrder(std::size_t coordinator_id, std::size_t partition_id, DatabaseType &db, const ContextType &context, RandomType &random,
		 Partitioner &partitioner, std::size_t ith_replica = 0)
		: Transaction(coordinator_id, partition_id, partitioner, ith_replica)
		, db(db)
		, context(context)
		, random(random)
		, partition_id(partition_id)
		, query(makeNewOrderQuery()(context, partition_id + 1, random))
	{
		storage = get_storage();
	}

	virtual ~NewOrder()
	{
		put_storage(storage);
		storage = nullptr;
	}

	virtual int32_t get_partition_count() override
	{
		return query.number_of_parts();
	}

	virtual int32_t get_partition(int i) override
	{
		return query.get_part(i);
	}

	virtual int32_t get_partition_granule_count(int i) override
	{
		return query.get_part_granule_count(i);
	}

	virtual int32_t get_granule(int partition_id, int j) override
	{
		return query.get_part_granule(partition_id, j);
	}

	virtual bool is_single_partition() override
	{
		return query.number_of_parts() == 1;
	}

	virtual const std::string serialize(std::size_t ith_replica = 0) override
	{
		std::string res;
		uint32_t txn_type = 0;
		Encoder encoder(res);
		encoder << this->transaction_id << txn_type << this->straggler_wait_time << ith_replica << this->txn_random_seed_start << partition_id;
		Transaction::serialize_lock_status(encoder);
		return res;
	}

	TransactionResult execute(std::size_t worker_id) override
	{
                storage->cleanup();
		std::size_t granules_per_partition = this->context.granules_per_partition;

		ScopedTimer t_local_work([&, this](uint64_t us) { this->record_local_work_time(us); });
		int32_t W_ID = this->partition_id + 1;

		// The input data (see Clause 2.4.3.2) are communicated to the SUT.

		int32_t D_ID = query.D_ID;
		int32_t C_ID = query.C_ID;

		// The row in the WAREHOUSE table with matching W_ID is selected and W_TAX,
		// the warehouse tax rate, is retrieved.

		auto warehouseTableID = warehouse::tableID;
		storage->warehouse_key = warehouse::key(W_ID);
		this->search_for_read(warehouseTableID, W_ID - 1, storage->warehouse_key, storage->warehouse_value, wid_to_granule_id(W_ID, context));

		// The row in the DISTRICT table with matching D_W_ID and D_ ID is selected,
		// D_TAX, the district tax rate, is retrieved, and D_NEXT_O_ID, the next
		// available order number for the district, is retrieved and incremented by
		// one.

		auto districtTableID = district::tableID;
		storage->district_key = district::key(W_ID, D_ID);
		this->search_for_update(districtTableID, W_ID - 1, storage->district_key, storage->district_value, did_to_granule_id(D_ID, context));

		// The row in the CUSTOMER table with matching C_W_ID, C_D_ID, and C_ID is
		// selected and C_DISCOUNT, the customer's discount rate, C_LAST, the
		// customer's last name, and C_CREDIT, the customer's credit status, are
		// retrieved.

		auto customerTableID = customer::tableID;
		storage->customer_key[0] = customer::key(W_ID, D_ID, C_ID);
		this->search_for_read(customerTableID, W_ID - 1, storage->customer_key[0], storage->customer_value[0], did_to_granule_id(D_ID, context));

		auto itemTableID = item::tableID;
		auto stockTableID = stock::tableID;

		for (int i = 0; i < query.O_OL_CNT; i++) {
			// The row in the ITEM table with matching I_ID (equals OL_I_ID) is
			// selected and I_PRICE, the price of the item, I_NAME, the name of the
			// item, and I_DATA are retrieved. If I_ID has an unused value (see
			// Clause 2.4.1.5), a "not-found" condition is signaled, resulting in a
			// rollback of the database transaction (see Clause 2.4.2.3).

			int32_t OL_I_ID = query.INFO[i].OL_I_ID;
			int8_t OL_QUANTITY = query.INFO[i].OL_QUANTITY;
			int32_t OL_SUPPLY_W_ID = query.INFO[i].OL_SUPPLY_W_ID;

			storage->item_keys[i] = item::key(OL_I_ID);

			// If I_ID has an unused value, rollback.
			// In OCC, rollback can return without going through commit protocal

			if (storage->item_keys[i].I_ID == 0) {
				// abort();
				return TransactionResult::ABORT_NORETRY;
			}

			this->search_local_index(itemTableID, 0, storage->item_keys[i], storage->item_values[i], true);

			// The row in the STOCK table with matching S_I_ID (equals OL_I_ID) and
			// S_W_ID (equals OL_SUPPLY_W_ID) is selected.

			storage->stock_keys[i] = stock::key(OL_SUPPLY_W_ID, OL_I_ID);

			this->search_for_update(stockTableID, OL_SUPPLY_W_ID - 1, storage->stock_keys[i], storage->stock_values[i],
						id_to_granule_id(OL_I_ID, context));
		}

		this->update(districtTableID, W_ID - 1, storage->district_key, storage->district_value, did_to_granule_id(D_ID, context));
		for (int i = 0; i < query.O_OL_CNT; i++) {
			int32_t OL_I_ID = query.INFO[i].OL_I_ID;
			int8_t OL_QUANTITY = query.INFO[i].OL_QUANTITY;
			int32_t OL_SUPPLY_W_ID = query.INFO[i].OL_SUPPLY_W_ID;

			this->update(stockTableID, OL_SUPPLY_W_ID - 1, storage->stock_keys[i], storage->stock_values[i], id_to_granule_id(OL_I_ID, context));
		}

		t_local_work.end();
		if (this->process_requests(worker_id)) {
			return TransactionResult::ABORT;
		}
		t_local_work.reset();

		float W_TAX = storage->warehouse_value.W_TAX;

		float D_TAX = storage->district_value.D_TAX;

		int32_t D_NEXT_O_ID = storage->district_value.D_NEXT_O_ID;
		storage->district_value.D_NEXT_O_ID += 1;

		if (context.operation_replication) {
			Encoder encoder(this->operation.data);
			this->operation.partition_id = this->partition_id;
			encoder << true << storage->district_key.D_W_ID << storage->district_key.D_ID << storage->district_value.D_NEXT_O_ID;
		}

		float C_DISCOUNT = storage->customer_value[0].C_DISCOUNT;

		// A new row is inserted into both the NEW-ORDER table and the ORDER table
		// to reflect the creation of the new order. O_CARRIER_ID is set to a null
		// value. If the order includes only home order-lines, then O_ALL_LOCAL is
		// set to 1, otherwise O_ALL_LOCAL is set to 0.

                auto newOrderTableID = new_order::tableID;
		storage->new_order_key = new_order::key(W_ID, D_ID, D_NEXT_O_ID);
                storage->new_order_value.NO_DUMMY = 0;
                this->insert_row(newOrderTableID, W_ID - 1, storage->new_order_key, storage->new_order_value, true);

                auto orderTableID = order::tableID;
		storage->order_key[0] = order::key(W_ID, D_ID, D_NEXT_O_ID);
		storage->order_value[0].O_ENTRY_D = Time::now();
		storage->order_value[0].O_CARRIER_ID = 0;
		storage->order_value[0].O_OL_CNT = query.O_OL_CNT;
		storage->order_value[0].O_C_ID = query.C_ID;
		storage->order_value[0].O_ALL_LOCAL = !query.isRemote();
                this->insert_row(orderTableID, W_ID - 1, storage->order_key[0], storage->order_value[0], true);

		float total_amount = 0;

		auto orderLineTableID = order_line::tableID;

		for (int i = 0; i < query.O_OL_CNT; i++) {
			int32_t OL_I_ID = query.INFO[i].OL_I_ID;
			int8_t OL_QUANTITY = query.INFO[i].OL_QUANTITY;
			int32_t OL_SUPPLY_W_ID = query.INFO[i].OL_SUPPLY_W_ID;

			float I_PRICE = storage->item_values[i].I_PRICE;

			// S_QUANTITY, the quantity in stock, S_DIST_xx, where xx represents the
			// district number, and S_DATA are retrieved. If the retrieved value for
			// S_QUANTITY exceeds OL_QUANTITY by 10 or more, then S_QUANTITY is
			// decreased by OL_QUANTITY; otherwise S_QUANTITY is updated to
			// (S_QUANTITY - OL_QUANTITY)+91. S_YTD is increased by OL_QUANTITY and
			// S_ORDER_CNT is incremented by 1. If the order-line is remote, then
			// S_REMOTE_CNT is incremented by 1.

			if (storage->stock_values[i].S_QUANTITY >= OL_QUANTITY + 10) {
				storage->stock_values[i].S_QUANTITY -= OL_QUANTITY;
			} else {
				storage->stock_values[i].S_QUANTITY = storage->stock_values[i].S_QUANTITY - OL_QUANTITY + 91;
			}

			storage->stock_values[i].S_YTD += OL_QUANTITY;
			storage->stock_values[i].S_ORDER_CNT++;

			if (OL_SUPPLY_W_ID != W_ID) {
				storage->stock_values[i].S_REMOTE_CNT++;
			}

			if (context.operation_replication) {
				Encoder encoder(this->operation.data);
				encoder << storage->stock_keys[i].S_W_ID << storage->stock_keys[i].S_I_ID << storage->stock_values[i].S_QUANTITY
					<< storage->stock_values[i].S_YTD << storage->stock_values[i].S_ORDER_CNT << storage->stock_values[i].S_REMOTE_CNT;
			}

			if (this->execution_phase) {
                                // The amount for the item in the order (OL_AMOUNT) is computed as: OL_QUANTITY * I_PRICE

				float OL_AMOUNT = I_PRICE * OL_QUANTITY;

                                // The strings in I_DATA and S_DATA are examined.
                                // If they both include the string "ORIGINAL", the brandgeneric field for that item is set to "B",
                                // otherwise, the brand-generic field is set to "G".
                                // What do you mean???

                                // A new row is inserted into the ORDER-LINE table to reflect the item on the order.
                                // OL_DELIVERY_D is set to a null value, OL_NUMBER is set to a unique value within all the ORDER-LINE rows that have the same OL_O_ID value,
                                // and OL_DIST_INFO is set to the content of S_DIST_xx, where xx represents the district number (OL_D_ID)

				storage->order_line_keys[0][i] = order_line::key(W_ID, D_ID, D_NEXT_O_ID, i + 1);

				storage->order_line_values[0][i].OL_I_ID = OL_I_ID;
				storage->order_line_values[0][i].OL_SUPPLY_W_ID = OL_SUPPLY_W_ID;
				storage->order_line_values[0][i].OL_DELIVERY_D = 0;
				storage->order_line_values[0][i].OL_QUANTITY = OL_QUANTITY;
				storage->order_line_values[0][i].OL_AMOUNT = OL_AMOUNT;

				switch (D_ID) {
				case 1:
					storage->order_line_values[0][i].OL_DIST_INFO = storage->stock_values[i].S_DIST_01;
					break;
				case 2:
					storage->order_line_values[0][i].OL_DIST_INFO = storage->stock_values[i].S_DIST_02;
					break;
				case 3:
					storage->order_line_values[0][i].OL_DIST_INFO = storage->stock_values[i].S_DIST_03;
					break;
				case 4:
					storage->order_line_values[0][i].OL_DIST_INFO = storage->stock_values[i].S_DIST_04;
					break;
				case 5:
					storage->order_line_values[0][i].OL_DIST_INFO = storage->stock_values[i].S_DIST_05;
					break;
				case 6:
					storage->order_line_values[0][i].OL_DIST_INFO = storage->stock_values[i].S_DIST_06;
					break;
				case 7:
					storage->order_line_values[0][i].OL_DIST_INFO = storage->stock_values[i].S_DIST_07;
					break;
				case 8:
					storage->order_line_values[0][i].OL_DIST_INFO = storage->stock_values[i].S_DIST_08;
					break;
				case 9:
					storage->order_line_values[0][i].OL_DIST_INFO = storage->stock_values[i].S_DIST_09;
					break;
				case 10:
					storage->order_line_values[0][i].OL_DIST_INFO = storage->stock_values[i].S_DIST_10;
					break;
				default:
					DCHECK(false);
					break;
				}

                                /* 
                                 * Our current code does not correctly handle repeated access within a single transaction - repeated read/write, overlapped scan,
                                 * and consecutive insert/delete.
                                 * This is because we did not implement the ownership of locks or placeholders so we cannot check if a lock is already held by the
                                 * same transaction or a placeholder is created by the same transaction.
                                 * Right now, for consecutive inserts, we explicitly specify that we only lock the last key.
                                 */
                                if (i == query.O_OL_CNT - 1)
                                        this->insert_row(orderLineTableID, W_ID - 1, storage->order_line_keys[0][i], storage->order_line_values[0][i], true);
                                else
                                        this->insert_row(orderLineTableID, W_ID - 1, storage->order_line_keys[0][i], storage->order_line_values[0][i], false);

				total_amount += OL_AMOUNT * (1 - C_DISCOUNT) * (1 + W_TAX + D_TAX);
			}
		}

                t_local_work.end();
		if (this->process_requests(worker_id)) {
			return TransactionResult::ABORT;
		}
		t_local_work.reset();

		return TransactionResult::READY_TO_COMMIT;
	}

	void reset_query() override
	{
		query = makeNewOrderQuery()(context, partition_id, random);
	}

    private:
	DatabaseType &db;
	const ContextType &context;
	RandomType random;
	Storage *storage = nullptr;
	std::size_t partition_id;
	NewOrderQuery query;

        uint64_t cur_sub_query_id;
};

template <class Transaction> class Payment : public Transaction {
    public:
	using DatabaseType = Database;
	using ContextType = typename DatabaseType::ContextType;
	using RandomType = typename DatabaseType::RandomType;
	using StorageType = Storage;

	Payment(std::size_t coordinator_id, std::size_t partition_id, DatabaseType &db, const ContextType &context, RandomType &random,
		Partitioner &partitioner, std::size_t ith_replica = 0)
		: Transaction(coordinator_id, partition_id, partitioner, ith_replica)
		, db(db)
		, context(context)
		, random(random)
		, partition_id(partition_id)
		, query(makePaymentQuery()(context, partition_id + 1, random))
	{
		storage = get_storage();
	}

	virtual ~Payment()
	{
		put_storage(storage);
		storage = nullptr;
	}

	virtual int32_t get_partition_count() override
	{
		return query.number_of_parts();
	}

	virtual int32_t get_partition(int i) override
	{
		return query.get_part(i);
	}

	virtual int32_t get_partition_granule_count(int i) override
	{
		return query.get_part_granule_count(i);
	}

	virtual int32_t get_granule(int partition_id, int j) override
	{
		return query.get_part_granule(partition_id, j);
	}

	virtual bool is_single_partition() override
	{
		return query.number_of_parts() == 1;
	}

	virtual const std::string serialize(std::size_t ith_replica = 0) override
	{
		std::string res;
		uint32_t txn_type = 1;
		Encoder encoder(res);
		encoder << this->transaction_id << txn_type << this->straggler_wait_time << ith_replica << this->txn_random_seed_start << partition_id;
		Transaction::serialize_lock_status(encoder);
		return res;
	}

	TransactionResult execute(std::size_t worker_id) override
	{
                storage->cleanup();
		ScopedTimer t_local_work([&, this](uint64_t us) { this->record_local_work_time(us); });
		int32_t W_ID = this->partition_id + 1;

		// The input data (see Clause 2.5.3.2) are communicated to the SUT.

		int32_t D_ID = query.D_ID;
		int32_t C_ID = query.C_ID;
		uint32_t old_CID = C_ID;
		int32_t C_D_ID = query.C_D_ID;
		int32_t C_W_ID = query.C_W_ID;
		float H_AMOUNT = query.H_AMOUNT;

		// The row in the WAREHOUSE table with matching W_ID is selected.
		// W_NAME, W_STREET_1, W_STREET_2, W_CITY, W_STATE, and W_ZIP are retrieved
		// and W_YTD,

		auto warehouseTableID = warehouse::tableID;
		storage->warehouse_key = warehouse::key(W_ID);
		this->search_for_update(warehouseTableID, W_ID - 1, storage->warehouse_key, storage->warehouse_value, wid_to_granule_id(W_ID, context));

		// The row in the DISTRICT table with matching D_W_ID and D_ID is selected.
		// D_NAME, D_STREET_1, D_STREET_2, D_CITY, D_STATE, and D_ZIP are retrieved
		// and D_YTD,

		auto districtTableID = district::tableID;
		storage->district_key = district::key(W_ID, D_ID);
		this->search_for_update(districtTableID, W_ID - 1, storage->district_key, storage->district_value, did_to_granule_id(D_ID, context));

		// The row in the CUSTOMER table with matching C_W_ID, C_D_ID, and C_ID is
		// selected and C_DISCOUNT, the customer's discount rate, C_LAST, the
		// customer's last name, and C_CREDIT, the customer's credit status, are
		// retrieved.

		auto customerNameIdxTableID = customer_name_idx::tableID;

		this->update(warehouseTableID, W_ID - 1, storage->warehouse_key, storage->warehouse_value, wid_to_granule_id(W_ID, context));
		this->update(districtTableID, W_ID - 1, storage->district_key, storage->district_value, did_to_granule_id(D_ID, context));
		if (C_ID == 0) {
			storage->customer_name_idx_key = customer_name_idx::key(C_W_ID, C_D_ID, query.C_LAST);
			this->search_local_index(customerNameIdxTableID, C_W_ID - 1, storage->customer_name_idx_key, storage->customer_name_idx_value, true);
			t_local_work.end();
			this->process_requests(worker_id, false);
			t_local_work.reset();
			C_ID = storage->customer_name_idx_value.C_ID;
		}

		auto customerTableID = customer::tableID;
		storage->customer_key[0] = customer::key(C_W_ID, C_D_ID, C_ID);
		this->search_for_update(customerTableID, C_W_ID - 1, storage->customer_key[0], storage->customer_value[0], did_to_granule_id(C_D_ID, context));
		this->update(customerTableID, C_W_ID - 1, storage->customer_key[0], storage->customer_value[0], did_to_granule_id(C_D_ID, context));

		t_local_work.end();
		if (this->process_requests(worker_id)) {
			return TransactionResult::ABORT;
		}
		ScopedTimer t_local_work2([&, this](uint64_t us) { this->record_local_work_time(us); });

		// the warehouse's year-to-date balance, is increased by H_AMOUNT.
		storage->warehouse_value.W_YTD += H_AMOUNT;

		if (context.operation_replication) {
			this->operation.partition_id = this->partition_id;
			Encoder encoder(this->operation.data);
			encoder << false << storage->warehouse_key.W_ID << storage->warehouse_value.W_YTD;
		}

		// the district's year-to-date balance, is increased by H_AMOUNT.
		storage->district_value.D_YTD += H_AMOUNT;

		if (context.operation_replication) {
			Encoder encoder(this->operation.data);
			encoder << storage->district_key.D_W_ID << storage->district_key.D_ID << storage->district_value.D_YTD;
		}

                // If the value of C_CREDIT is equal to "BC", then C_DATA is also retrieved from the selected customer and the following history information:
                // C_ID, C_D_ID, C_W_ID, D_ID, W_ID, and H_AMOUNT, are inserted at the left of the C_DATA field by shifting the existing content of C_DATA
                // to the right by an equal number of bytes and by discarding the bytes that are shifted out of the right side of the C_DATA field.
                // The content of the C_DATA field never exceeds 500 characters. The selected customer is updated with the new C_DATA field.
                // If C_DATA is implemented as two fields (see Clause 1.4.9), they must be treated and operated on as one single field.

		char C_DATA[501];
		int total_written = 0;
		if (this->execution_phase) {
			if (storage->customer_value[0].C_CREDIT == "BC") {
				int written;

				written = std::sprintf(C_DATA + total_written, "%d ", C_ID);
				total_written += written;

				written = std::sprintf(C_DATA + total_written, "%d ", C_D_ID);
				total_written += written;

				written = std::sprintf(C_DATA + total_written, "%d ", C_W_ID);
				total_written += written;

				written = std::sprintf(C_DATA + total_written, "%d ", D_ID);
				total_written += written;

				written = std::sprintf(C_DATA + total_written, "%d ", W_ID);
				total_written += written;

				written = std::sprintf(C_DATA + total_written, "%.2f ", H_AMOUNT);
				total_written += written;

				const char *old_C_DATA = storage->customer_value[0].C_DATA.c_str();

				std::memcpy(C_DATA + total_written, old_C_DATA, 500 - total_written);
				C_DATA[500] = 0;

				storage->customer_value[0].C_DATA.assign(C_DATA);
			}

			storage->customer_value[0].C_BALANCE -= H_AMOUNT;
			storage->customer_value[0].C_YTD_PAYMENT += H_AMOUNT;
			storage->customer_value[0].C_PAYMENT_CNT += 1;
		}

		if (context.operation_replication) {
			Encoder encoder(this->operation.data);
			encoder << storage->customer_key[0].C_W_ID << storage->customer_key[0].C_D_ID << storage->customer_key[0].C_ID;
			encoder << uint32_t(total_written);
			encoder.write_n_bytes(C_DATA, total_written);
			encoder << storage->customer_value[0].C_BALANCE << storage->customer_value[0].C_YTD_PAYMENT << storage->customer_value[0].C_PAYMENT_CNT;
		}

		char H_DATA[25];
		int written;
		if (this->execution_phase) {
                        // H_DATA is built by concatenating W_NAME and D_NAME separated by 4 spaces.

			written = std::sprintf(H_DATA, "%s    %s", storage->warehouse_value.W_NAME.c_str(), storage->district_value.D_NAME.c_str());
			H_DATA[written] = 0;

                        // A new row is inserted into the HISTORY table with
                        // H_C_ID = C_ID, H_C_D_ID = C_D_ID, H_C_W_ID = C_W_ID, H_D_ID = D_ID, and H_W_ID = W_ID.

                        auto historyTableID = history::tableID;
			storage->history_key = history::key(W_ID, D_ID, C_W_ID, C_D_ID, C_ID, Time::now());
			storage->history_value.H_AMOUNT = H_AMOUNT;
			storage->history_value.H_DATA.assign(H_DATA, written);

                        this->insert_row(historyTableID, W_ID - 1, storage->history_key, storage->history_value, true);
		}

                t_local_work.end();
		if (this->process_requests(worker_id)) {
			return TransactionResult::ABORT;
		}
		t_local_work.reset();

		return TransactionResult::READY_TO_COMMIT;
	}

	void reset_query() override
	{
		query = makePaymentQuery()(context, partition_id, random);
	}

    private:
	DatabaseType &db;
	const ContextType &context;
	RandomType random;
	Storage *storage;
	std::size_t partition_id;
	PaymentQuery query;
};

template <class Transaction> class OrderStatus : public Transaction {
    public:
	using DatabaseType = Database;
	using ContextType = typename DatabaseType::ContextType;
	using RandomType = typename DatabaseType::RandomType;
	using StorageType = Storage;

	OrderStatus(std::size_t coordinator_id, std::size_t partition_id, DatabaseType &db, const ContextType &context, RandomType &random,
		Partitioner &partitioner, std::size_t ith_replica = 0)
		: Transaction(coordinator_id, partition_id, partitioner, ith_replica)
		, db(db)
		, context(context)
		, random(random)
		, partition_id(partition_id)
		, query(makeOrderStatusQuery()(context, partition_id + 1, random))
	{
		storage = get_storage();
	}

	virtual ~OrderStatus()
	{
		put_storage(storage);
		storage = nullptr;
	}

	virtual int32_t get_partition_count() override
	{
		return query.number_of_parts();
	}

	virtual int32_t get_partition(int i) override
	{
		return query.get_part(i);
	}

	virtual int32_t get_partition_granule_count(int i) override
	{
		return query.get_part_granule_count(i);
	}

	virtual int32_t get_granule(int partition_id, int j) override
	{
		return query.get_part_granule(partition_id, j);
	}

	virtual bool is_single_partition() override
	{
		return query.number_of_parts() == 1;
	}

	virtual const std::string serialize(std::size_t ith_replica = 0) override
	{
		std::string res;
		uint32_t txn_type = 1;
		Encoder encoder(res);
		encoder << this->transaction_id << txn_type << this->straggler_wait_time << ith_replica << this->txn_random_seed_start << partition_id;
		Transaction::serialize_lock_status(encoder);
		return res;
	}

	TransactionResult execute(std::size_t worker_id) override
	{
                storage->cleanup();
		ScopedTimer t_local_work([&, this](uint64_t us) { this->record_local_work_time(us); });
		int32_t W_ID = this->partition_id + 1;

		// The input data (see Clause 2.5.3.2) are communicated to the SUT.

		int32_t D_ID = query.D_ID;
		int32_t C_ID = query.C_ID;
		uint32_t old_CID = C_ID;
                int32_t C_W_ID = query.C_W_ID;
		int32_t C_D_ID = query.C_D_ID;

		// The row in the CUSTOMER table with matching C_W_ID, C_D_ID, and C_ID is
		// selected and C_BALANCE, C_FIRST, C_MIDDLE, and C_LAST are retrieved.

		auto customerNameIdxTableID = customer_name_idx::tableID;

		if (C_ID == 0) {
			storage->customer_name_idx_key = customer_name_idx::key(C_W_ID, C_D_ID, query.C_LAST);
			this->search_local_index(customerNameIdxTableID, C_W_ID - 1, storage->customer_name_idx_key, storage->customer_name_idx_value, true);
			t_local_work.end();
			this->process_requests(worker_id, false);
			t_local_work.reset();
			C_ID = storage->customer_name_idx_value.C_ID;
		}

		auto customerTableID = customer::tableID;
		storage->customer_key[0] = customer::key(C_W_ID, C_D_ID, C_ID);
		this->search_for_read(customerTableID, C_W_ID - 1, storage->customer_key[0], storage->customer_value[0], did_to_granule_id(C_D_ID, context));

                // The row in the ORDER table with matching O_W_ID (equals C_W_ID), O_D_ID (equals C_D_ID), O_C_ID (equals C_ID),
                // and with the largest existing O_ID, is selected. This is the most recent order placed by that customer.
                // O_ID, O_ENTRY_D, and O_CARRIER_ID are retrieved.

                int32_t O_W_ID = query.C_W_ID;
                int32_t O_D_ID = query.C_D_ID;
                int32_t O_C_ID = C_ID;

                auto orderCustTableID = order_customer::tableID;
                storage->min_order_customer_key = order_customer::key(O_W_ID, O_D_ID, O_C_ID, 0);
                storage->max_order_customer_key = order_customer::key(O_W_ID, O_D_ID, O_C_ID, MAX_ORDER_ID);
                this->scan_for_read(orderCustTableID, O_W_ID - 1, storage->min_order_customer_key, storage->max_order_customer_key,
                                0, &storage->order_customer_scan_results, did_to_granule_id(C_D_ID, context));

		t_local_work.end();
		if (this->process_requests(worker_id)) {
			return TransactionResult::ABORT;
		}
		t_local_work.reset();

                // get the last order's ID
                CHECK(storage->order_customer_scan_results.size() > 0);
                auto last_order = storage->order_customer_scan_results[storage->order_customer_scan_results.size() - 1];
                const auto last_order_key = *reinterpret_cast<order_customer::key *>(last_order.key);
                auto last_order_id = last_order_key.O_ID;

                // All rows in the ORDER-LINE table with matching OL_W_ID (equals O_W_ID), OL_D_ID (equals O_D_ID), and OL_O_ID (equals O_ID) are
                // selected and the corresponding sets of OL_I_ID, OL_SUPPLY_W_ID, OL_QUANTITY, OL_AMOUNT, and OL_DELIVERY_D are retrieved.

                auto orderLineTableID = order_line::tableID;
                storage->min_order_line_key[0] = order_line::key(O_W_ID, O_D_ID, last_order_id, 1);
                storage->max_order_line_key[0] = order_line::key(O_W_ID, O_D_ID, last_order_id, MAX_ORDER_LINE_PER_ORDER);
                this->scan_for_read(orderLineTableID, O_W_ID - 1, storage->min_order_line_key[0], storage->max_order_line_key[0],
                                0, &storage->order_line_scan_results[0], did_to_granule_id(C_D_ID, context));

                t_local_work.end();
                if (this->process_requests(worker_id)) {
			return TransactionResult::ABORT;
		}
                t_local_work.reset();

		return TransactionResult::READY_TO_COMMIT;
	}

	void reset_query() override
	{
		query = makeOrderStatusQuery()(context, partition_id, random);
	}

    private:
	DatabaseType &db;
	const ContextType &context;
	RandomType random;
	Storage *storage;
	std::size_t partition_id;
	OrderStatusQuery query;
};

template <class Transaction> class Delivery : public Transaction {
    public:
	using DatabaseType = Database;
	using ContextType = typename DatabaseType::ContextType;
	using RandomType = typename DatabaseType::RandomType;
	using StorageType = Storage;

	Delivery(std::size_t coordinator_id, std::size_t partition_id, DatabaseType &db, const ContextType &context, RandomType &random,
		Partitioner &partitioner, uint64_t sub_query_id, std::size_t ith_replica = 0)
		: Transaction(coordinator_id, partition_id, partitioner, ith_replica)
		, db(db)
		, context(context)
		, random(random)
		, partition_id(partition_id)
		, query(makeDeliveryQuery()(context, partition_id + 1, random, sub_query_id))
	{
		storage = get_storage();
                CHECK(sub_query_id >= 0 && sub_query_id < DISTRICT_PER_WAREHOUSE);
	}

	virtual ~Delivery()
	{
		put_storage(storage);
		storage = nullptr;
	}

	virtual int32_t get_partition_count() override
	{
		return query.number_of_parts();
	}

	virtual int32_t get_partition(int i) override
	{
		return query.get_part(i);
	}

	virtual int32_t get_partition_granule_count(int i) override
	{
		return query.get_part_granule_count(i);
	}

	virtual int32_t get_granule(int partition_id, int j) override
	{
		return query.get_part_granule(partition_id, j);
	}

	virtual bool is_single_partition() override
	{
		return query.number_of_parts() == 1;
	}

	virtual const std::string serialize(std::size_t ith_replica = 0) override
	{
		std::string res;
		uint32_t txn_type = 1;
		Encoder encoder(res);
		encoder << this->transaction_id << txn_type << this->straggler_wait_time << ith_replica << this->txn_random_seed_start << partition_id;
		Transaction::serialize_lock_status(encoder);
		return res;
	}

	TransactionResult execute(std::size_t worker_id) override
	{
                storage->cleanup();
		ScopedTimer t_local_work([&, this](uint64_t us) { this->record_local_work_time(us); });
		int32_t W_ID = this->partition_id + 1;
                uint64_t cur_district_id = query.sub_query_id + 1;

		// The input data (see Clause 2.5.3.2) are communicated to the SUT.

		int32_t O_CARRIER_ID = query.O_CARRIER_ID;
		uint64_t OL_DELIVERY_D = query.OL_DELIVERY_D;

                // For a given warehouse number (W_ID), for each of the 10 districts (D_W_ID , D_ID) within that warehouse,
                // and for a given carrier number (O_CARRIER_ID):

                // for (uint64_t D_ID = 1; D_ID <= DISTRICT_PER_WAREHOUSE; D_ID++) {
                for (uint64_t D_ID = cur_district_id; D_ID == cur_district_id; D_ID++) {
                        // The row in the NEW-ORDER table with matching NO_W_ID (equals W_ID) and NO_D_ID (equals D_ID)
                        // and with the lowest NO_O_ID value is selected.
                        // This is the oldest undelivered order of that district. NO_O_ID, the order number, is retrieved.
                        // If no matching row is found, then the delivery of an order for this district is skipped.

                        auto newOrderTableID = new_order::tableID;
                        storage->min_new_order_key[D_ID - 1] = new_order::key(W_ID, D_ID, 0);
                        storage->max_new_order_key[D_ID - 1] = new_order::key(W_ID, D_ID, MAX_ORDER_ID);
                        this->scan_for_delete(newOrderTableID, W_ID - 1, storage->min_new_order_key[D_ID - 1], storage->max_new_order_key[D_ID - 1],
                                1, &storage->new_order_scan_results[D_ID - 1], did_to_granule_id(D_ID, context));

                        t_local_work.end();
                        if (this->process_requests(worker_id)) {
                                return TransactionResult::ABORT;
                        }
                        t_local_work.reset();

                        if (storage->new_order_scan_results[D_ID - 1].size() == 0) {
                                // We are not supposed to abort here, since this case is allowed by the spec.
                                // But there is probably something wrong, so I just abort.
                                CHECK(0);
                                return TransactionResult::READY_TO_COMMIT;
                        }

                        auto oldest_undelivered_order = storage->new_order_scan_results[D_ID - 1][0];
                        storage->oldest_undelivered_order_key = *reinterpret_cast<new_order::key *>(oldest_undelivered_order.key);

                        // The selected row in the NEW-ORDER table is deleted.

                        this->delete_row(newOrderTableID, W_ID - 1, storage->oldest_undelivered_order_key);

                        // The row in the ORDER table with matching O_W_ID (equals W_ID), O_D_ID (equals D_ID), and O_ID (equals NO_O_ID) is selected,
                        // O_C_ID, the customer number, is retrieved, and O_CARRIER_ID is updated.

                        auto orderTableID = order::tableID;
		        storage->order_key[D_ID - 1] = order::key(W_ID, D_ID, storage->oldest_undelivered_order_key.NO_O_ID);
		        this->search_for_update(orderTableID, W_ID - 1, storage->order_key[D_ID - 1], storage->order_value[D_ID - 1], did_to_granule_id(D_ID, context));
                        this->update(orderTableID, W_ID - 1, storage->order_key[D_ID - 1], storage->order_value[D_ID - 1], did_to_granule_id(D_ID, context));

                        // All rows in the ORDER-LINE table with matching OL_W_ID (equals O_W_ID), OL_D_ID (equals O_D_ID), and OL_O_ID (equals O_ID) 
                        // are selected. All OL_DELIVERY_D, the delivery dates, are updated to the current system time as returned by the operating system
                        // and the sum of all OL_AMOUNT is retrieved.

                        auto orderLineTableID = order_line::tableID;
                        storage->min_order_line_key[D_ID - 1] = order_line::key(W_ID, D_ID, storage->oldest_undelivered_order_key.NO_O_ID, 1);
                        storage->max_order_line_key[D_ID - 1] = order_line::key(W_ID, D_ID, storage->oldest_undelivered_order_key.NO_O_ID, MAX_ORDER_LINE_PER_ORDER);

                        this->scan_for_update(orderLineTableID, W_ID - 1, storage->min_order_line_key[D_ID - 1], storage->max_order_line_key[D_ID - 1],
                                        0, &storage->order_line_scan_results[D_ID - 1], did_to_granule_id(D_ID, context));

                        t_local_work.end();
                        if (this->process_requests(worker_id)) {
                                return TransactionResult::ABORT;
                        }
                        t_local_work.reset();

                        storage->order_value[D_ID - 1].O_CARRIER_ID = query.O_CARRIER_ID;

                        // The row in the CUSTOMER table with matching C_W_ID (equals W_ID), C_D_ID (equals D_ID), and C_ID (equals O_C_ID) is selected and
                        // C_BALANCE is increased by the sum of all order-line amounts (OL_AMOUNT) previously retrieved. C_DELIVERY_CNT is incremented by 1.

                        auto C_ID = storage->order_value[D_ID - 1].O_C_ID;        // get C_ID
                        auto customerTableID = customer::tableID;
                        storage->customer_key[D_ID - 1] = customer::key(W_ID, D_ID, C_ID);
                        this->search_for_update(customerTableID, W_ID - 1, storage->customer_key[D_ID - 1], storage->customer_value[D_ID - 1], did_to_granule_id(D_ID, context));
                        this->update(customerTableID, W_ID - 1, storage->customer_key[D_ID - 1], storage->customer_value[D_ID - 1], did_to_granule_id(D_ID, context));

                        t_local_work.end();
                        if (this->process_requests(worker_id)) {
                                return TransactionResult::ABORT;
                        }
                        t_local_work.reset();

                        uint64_t sum_ol_amount = 0;
                        for (int i = 0; i < storage->order_line_scan_results[D_ID - 1].size(); i++) {
                                storage->order_line_values[D_ID - 1][i].OL_DELIVERY_D = Time::now();
                                sum_ol_amount += storage->order_line_values[D_ID - 1][i].OL_AMOUNT;
                        }

                        storage->customer_value[D_ID - 1].C_BALANCE += sum_ol_amount;
                        storage->customer_value[D_ID - 1].C_DELIVERY_CNT++;
                }

		return TransactionResult::READY_TO_COMMIT;
	}

	void reset_query() override
	{
		query = makeDeliveryQuery()(context, partition_id, random, query.sub_query_id);
	}

    private:
	DatabaseType &db;
	const ContextType &context;
	RandomType random;
	Storage *storage;
	std::size_t partition_id;
	DeliveryQuery query;
};

template <class Transaction> class StockLevel : public Transaction {
    public:
	using DatabaseType = Database;
	using ContextType = typename DatabaseType::ContextType;
	using RandomType = typename DatabaseType::RandomType;
	using StorageType = Storage;

	StockLevel(std::size_t coordinator_id, std::size_t partition_id, DatabaseType &db, const ContextType &context, RandomType &random,
		Partitioner &partitioner, std::size_t ith_replica = 0)
		: Transaction(coordinator_id, partition_id, partitioner, ith_replica)
		, db(db)
		, context(context)
		, random(random)
		, partition_id(partition_id)
		, query(makeStockLevelQuery()(context, partition_id + 1, random))
	{
		storage = get_storage();
	}

	virtual ~StockLevel()
	{
		put_storage(storage);
		storage = nullptr;
	}

	virtual int32_t get_partition_count() override
	{
		return query.number_of_parts();
	}

	virtual int32_t get_partition(int i) override
	{
		return query.get_part(i);
	}

	virtual int32_t get_partition_granule_count(int i) override
	{
		return query.get_part_granule_count(i);
	}

	virtual int32_t get_granule(int partition_id, int j) override
	{
		return query.get_part_granule(partition_id, j);
	}

	virtual bool is_single_partition() override
	{
		return query.number_of_parts() == 1;
	}

	virtual const std::string serialize(std::size_t ith_replica = 0) override
	{
		std::string res;
		uint32_t txn_type = 1;
		Encoder encoder(res);
		encoder << this->transaction_id << txn_type << this->straggler_wait_time << ith_replica << this->txn_random_seed_start << partition_id;
		Transaction::serialize_lock_status(encoder);
		return res;
	}

	TransactionResult execute(std::size_t worker_id) override
	{
                storage->cleanup();
		ScopedTimer t_local_work([&, this](uint64_t us) { this->record_local_work_time(us); });
		int32_t W_ID = this->partition_id + 1;
                int32_t D_ID = query.D_ID;

                // The row in the DISTRICT table with matching D_W_ID and D_ID is selected and D_NEXT_O_ID is retrieved.

                auto districtTableID = district::tableID;
		storage->district_key = district::key(W_ID, D_ID);
		this->search_for_read(districtTableID, W_ID - 1, storage->district_key, storage->district_value, did_to_granule_id(D_ID, context));

                t_local_work.end();
                if (this->process_requests(worker_id)) {
                        return TransactionResult::ABORT;
                }
                t_local_work.reset();

                auto D_NEXT_O_ID = storage->district_value.D_NEXT_O_ID;

                // All rows in the ORDER-LINE table with matching OL_W_ID (equals W_ID), OL_D_ID (equals D_ID),
                // and OL_O_ID (lower than D_NEXT_O_ID and greater than or equal to D_NEXT_O_ID minus 20) are selected.
                // They are the items for 20 recent orders of the district.

                auto orderLineTableID = order_line::tableID;
                storage->min_order_line_key[0] = order_line::key(W_ID, D_ID, D_NEXT_O_ID - 20, 1);
                storage->max_order_line_key[0] = order_line::key(W_ID, D_ID, D_NEXT_O_ID, MAX_ORDER_LINE_PER_ORDER);
                this->scan_for_read(orderLineTableID, W_ID - 1, storage->min_order_line_key[0], storage->max_order_line_key[0],
                                0, &storage->order_line_scan_results[0], did_to_granule_id(D_ID, context));

                t_local_work.end();
                if (this->process_requests(worker_id)) {
                        return TransactionResult::ABORT;
                }
                t_local_work.reset();

                // All rows in the STOCK table with matching S_I_ID (equals OL_I_ID) and S_W_ID (equals W_ID) from the
                // list of distinct item numbers and with S_QUANTITY lower than threshold are counted (giving low_stock).

                // Stocks must be counted only for distinct items.
                // Thus, items that have been ordered more than once in the 20 selected orders 
                // must be aggregated into a single summary count for that item.

                std::unordered_set<int32_t> item_id_set;
                for (int i = 0; i < storage->order_line_scan_results[0].size(); i++) {
                        auto order_line = storage->order_line_scan_results[0][i];
                        const auto order_line_value = *reinterpret_cast<const order_line::value *>(order_line.data);
                        item_id_set.insert(order_line_value.OL_I_ID);
                }

                auto stockTableID = stock::tableID;
                auto S_W_ID = W_ID;

                int i = 0;
                for (auto item_id : item_id_set) {
                        auto S_I_ID = item_id;
                        storage->stock_keys[i] = stock::key(S_W_ID, S_I_ID);
                        this->search_for_read(stockTableID, W_ID - 1, storage->stock_keys[i], storage->stock_values[i], did_to_granule_id(D_ID, context));
                        i++;
                }

                t_local_work.end();
                if (this->process_requests(worker_id)) {
                        return TransactionResult::ABORT;
                }
                t_local_work.reset();

		return TransactionResult::READY_TO_COMMIT;
	}

	void reset_query() override
	{
		query = makeStockLevelQuery()(context, partition_id, random);
	}

    private:
	DatabaseType &db;
	const ContextType &context;
	RandomType random;
	Storage *storage;
	std::size_t partition_id;
	StockLevelQuery query;
};

template <class Transaction> class Test : public Transaction {
    public:
	using DatabaseType = Database;
	using ContextType = typename DatabaseType::ContextType;
	using RandomType = typename DatabaseType::RandomType;
	using StorageType = Storage;

	Test(std::size_t coordinator_id, std::size_t partition_id, DatabaseType &db, const ContextType &context, RandomType &random,
		Partitioner &partitioner, std::size_t ith_replica = 0)
		: Transaction(coordinator_id, partition_id, partitioner, ith_replica)
		, db(db)
		, context(context)
		, random(random)
		, partition_id(partition_id)
		, query(makeTestQuery()(context, partition_id + 1, random))
	{
		storage = get_storage();
	}

	virtual ~Test()
	{
		put_storage(storage);
		storage = nullptr;
	}

	virtual int32_t get_partition_count() override
	{
		return query.number_of_parts();
	}

	virtual int32_t get_partition(int i) override
	{
		return query.get_part(i);
	}

	virtual int32_t get_partition_granule_count(int i) override
	{
		return query.get_part_granule_count(i);
	}

	virtual int32_t get_granule(int partition_id, int j) override
	{
		return query.get_part_granule(partition_id, j);
	}

	virtual bool is_single_partition() override
	{
		return query.number_of_parts() == 1;
	}

	virtual const std::string serialize(std::size_t ith_replica = 0) override
	{
		std::string res;
		uint32_t txn_type = 1;
		Encoder encoder(res);
		encoder << this->transaction_id << txn_type << this->straggler_wait_time << ith_replica << this->txn_random_seed_start << partition_id;
		Transaction::serialize_lock_status(encoder);
		return res;
	}

	TransactionResult execute(std::size_t worker_id) override
	{
                storage->cleanup();
		ScopedTimer t_local_work([&, this](uint64_t us) { this->record_local_work_time(us); });
		int32_t W_ID = query.W_ID;
                int32_t D_ID = query.D_ID;
                int32_t C_ID = query.C_ID;

                CHECK(W_ID > 0);
                CHECK(D_ID == 1);
                CHECK(C_ID == 1);

                // The row in the CUSTOMER table with matching C_W_ID, C_D_ID, and C_ID is selected

		auto customerTableID = customer::tableID;
		storage->customer_key[0] = customer::key(W_ID, D_ID, C_ID);
		this->search_for_update(customerTableID, W_ID - 1, storage->customer_key[0], storage->customer_value[0], did_to_granule_id(D_ID, context));
		this->update(customerTableID, W_ID - 1, storage->customer_key[0], storage->customer_value[0], did_to_granule_id(D_ID, context));

                t_local_work.end();
                if (this->process_requests(worker_id)) {
                        return TransactionResult::ABORT;
                }
                t_local_work.reset();

                // increment C_PAYMENT_CNT, whose inital value is 1
                storage->customer_value[0].C_PAYMENT_CNT++;

		return TransactionResult::READY_TO_COMMIT;
	}

	void reset_query() override
	{
		query = makeTestQuery()(context, partition_id, random);
	}

    private:
	DatabaseType &db;
	const ContextType &context;
	RandomType random;
	Storage *storage;
	std::size_t partition_id;
	TestQuery query;
};

} // namespace tpcc
} // namespace star

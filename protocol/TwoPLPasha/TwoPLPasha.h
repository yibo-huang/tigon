//
// Created by Yi Lu on 9/11/18.
//

#pragma once

#include <algorithm>
#include <atomic>
#include <thread>

#include "core/Partitioner.h"
#include "core/Table.h"
#include "protocol/TwoPLPasha/TwoPLPashaHelper.h"
#include "protocol/TwoPLPasha/TwoPLPashaMessage.h"
#include "protocol/TwoPLPasha/TwoPLPashaTransaction.h"
#include <glog/logging.h>

namespace star
{

template <class Database> class TwoPLPasha {
    public:
	using DatabaseType = Database;
	using MetaDataType = std::atomic<uint64_t>;
	using ContextType = typename DatabaseType::ContextType;
	using MessageType = TwoPLPashaMessage;
	using TransactionType = TwoPLPashaTransaction;

	using MessageFactoryType = TwoPLPashaMessageFactory;
	using MessageHandlerType = TwoPLPashaMessageHandler;

	TwoPLPasha(DatabaseType &db, const ContextType &context, Partitioner &partitioner)
		: db(db)
		, context(context)
		, partitioner(partitioner)
	{
	}

	uint64_t generate_tid(TransactionType &txn)
	{
		auto &readSet = txn.readSet;
		auto &writeSet = txn.writeSet;

		uint64_t next_tid = 0;

		// larger than the TID of any record read or written by the transaction

		for (std::size_t i = 0; i < readSet.size(); i++) {
			next_tid = std::max(next_tid, readSet[i].get_tid());
		}

		// larger than the worker's most recent chosen TID

		next_tid = std::max(next_tid, max_tid);

		// increment

		next_tid++;

		// update worker's most recent chosen TID

		max_tid = next_tid;

		return next_tid;
	}

	void abort(TransactionType &txn, std::vector<std::unique_ptr<Message> > &messages)
	{
                if (this->context.enable_phantom_detection == true) {
                        // rollback inserts
                        auto &insertSet = txn.insertSet;

                        for (auto i = 0u; i < insertSet.size(); i++) {
                                auto &insertKey = insertSet[i];

                                if (insertKey.get_processed() == false)
                                        continue;

                                auto tableId = insertKey.get_table_id();
                                auto partitionId = insertKey.get_partition_id();
                                auto table = db.find_table(tableId, partitionId);
                                if (partitioner.has_master_partition(partitionId)) {
                                        // remove the placeholder
                                        auto key = insertKey.get_key();
                                        bool success = table->remove(key);
                                        DCHECK(success == true);

                                        // release the read lock for the next row
                                        if (insertKey.get_next_row_locked() == true) {
                                                auto next_row_entity = insertKey.get_next_row_entity();
                                                std::atomic<uint64_t> *next_key_meta = next_row_entity.meta;
                                                DCHECK(next_key_meta != nullptr);
                                                TwoPLPashaHelper::write_lock_release(*next_key_meta);
                                        }
                                } else {
                                        // currently the execution logic should never reach here
                                        DCHECK(0);
                                }
                        }

                        // rollback deletes - nothing needs to be done
                        // write locks will be released in writeSet and scanSet

                        // assume all writes are updates
                        auto &readSet = txn.readSet;

                        // release locks in the read/write set
                        for (auto i = 0u; i < readSet.size(); i++) {
                                // release read locks in the readSet
                                auto &readKey = readSet[i];
                                auto tableId = readKey.get_table_id();
                                auto partitionId = readKey.get_partition_id();
                                auto table = db.find_table(tableId, partitionId);
                                if (readKey.get_read_lock_bit()) {
                                        if (partitioner.has_master_partition(partitionId)) {
                                                auto cached_row = readKey.get_cached_local_row();
                                                DCHECK(std::get<0>(cached_row) != nullptr && std::get<1>(cached_row) != nullptr);

                                                // model CXL search overhead
                                                if (this->context.model_cxl_search_overhead == true) {
                                                        twopl_pasha_global_helper->model_cxl_search_overhead(cached_row, tableId, partitionId, readKey.get_key());
                                                }

                                                TwoPLPashaHelper::read_lock_release(*std::get<0>(cached_row));
                                        } else {
                                                char *migrated_row = readKey.get_cached_migrated_row();
                                                DCHECK(migrated_row != nullptr);
                                                TwoPLPashaHelper::remote_read_lock_release(migrated_row);
                                        }
                                }

                                // release write locks in the writeSet
                                if (readKey.get_write_lock_bit()) {
                                        if (partitioner.has_master_partition(partitionId)) {
                                                auto cached_row = readKey.get_cached_local_row();
                                                DCHECK(std::get<0>(cached_row) != nullptr && std::get<1>(cached_row) != nullptr);

                                                // model CXL search overhead
                                                if (this->context.model_cxl_search_overhead == true) {
                                                        twopl_pasha_global_helper->model_cxl_search_overhead(cached_row, tableId, partitionId, readKey.get_key());
                                                }

                                                TwoPLPashaHelper::write_lock_release(*std::get<0>(cached_row));
                                        } else {
                                                char *migrated_row = readKey.get_cached_migrated_row();
                                                DCHECK(migrated_row != nullptr);
                                                TwoPLPashaHelper::remote_write_lock_release(migrated_row);
                                        }
                                }
                        }

                        // release locks in the scan set
                        auto &scanSet = txn.scanSet;

                        for (auto i = 0u; i < scanSet.size(); i++) {
                                auto &scanKey = scanSet[i];
                                auto tableId = scanKey.get_table_id();
                                auto partitionId = scanKey.get_partition_id();
                                auto table = db.find_table(tableId, partitionId);
                                std::vector<ITable::row_entity> &scan_results = *reinterpret_cast<std::vector<ITable::row_entity> *>(scanKey.get_scan_res_vec());

                                // release the next row
                                if (scanKey.get_next_row_locked() == true) {
                                        if (partitioner.has_master_partition(partitionId)) {
                                                auto next_row_entity = scanKey.get_next_row_entity();
                                                std::atomic<uint64_t> *meta = next_row_entity.meta;
                                                DCHECK(meta != nullptr);
                                                if (scanKey.get_request_type() == TwoPLPashaRWKey::SCAN_FOR_READ) {
                                                        TwoPLPashaHelper::read_lock_release(*meta);
                                                } else if (scanKey.get_request_type() == TwoPLPashaRWKey::SCAN_FOR_UPDATE) {
                                                        TwoPLPashaHelper::write_lock_release(*meta);
                                                } else if (scanKey.get_request_type() == TwoPLPashaRWKey::SCAN_FOR_INSERT) {
                                                        TwoPLPashaHelper::write_lock_release(*meta);
                                                } else if (scanKey.get_request_type() == TwoPLPashaRWKey::SCAN_FOR_DELETE) {
                                                        TwoPLPashaHelper::write_lock_release(*meta);
                                                } else {
                                                        DCHECK(0);
                                                }
                                        } else {
                                                auto next_row_entity = scanKey.get_next_row_entity();
                                                char *cxl_row = reinterpret_cast<char *>(next_row_entity.meta);
                                                DCHECK(cxl_row != nullptr);
                                                if (scanKey.get_request_type() == TwoPLPashaRWKey::SCAN_FOR_READ) {
                                                        TwoPLPashaHelper::remote_read_lock_release(cxl_row);
                                                } else if (scanKey.get_request_type() == TwoPLPashaRWKey::SCAN_FOR_UPDATE) {
                                                        TwoPLPashaHelper::remote_write_lock_release(cxl_row);
                                                } else if (scanKey.get_request_type() == TwoPLPashaRWKey::SCAN_FOR_INSERT) {
                                                        TwoPLPashaHelper::remote_write_lock_release(cxl_row);
                                                } else if (scanKey.get_request_type() == TwoPLPashaRWKey::SCAN_FOR_DELETE) {
                                                        TwoPLPashaHelper::remote_write_lock_release(cxl_row);
                                                } else {
                                                        DCHECK(0);
                                                }
                                        }
                                }

                                if (scanKey.get_request_type() == TwoPLPashaRWKey::SCAN_FOR_READ) {
                                        // release read locks
                                        if (partitioner.has_master_partition(partitionId)) {
                                                for (auto i = 0u; i < scan_results.size(); i++) {
                                                        std::atomic<uint64_t> *meta = scan_results[i].meta;
                                                        DCHECK(meta != nullptr);
                                                        TwoPLPashaHelper::read_lock_release(*meta);
                                                }
                                        } else {
                                                for (auto i = 0u; i < scan_results.size(); i++) {
                                                        char *cxl_row = reinterpret_cast<char *>(scan_results[i].meta);
                                                        DCHECK(cxl_row != nullptr);
                                                        TwoPLPashaHelper::remote_read_lock_release(cxl_row);
                                                }
                                        }
                                } else if (scanKey.get_request_type() == TwoPLPashaRWKey::SCAN_FOR_UPDATE ||
                                        scanKey.get_request_type() == TwoPLPashaRWKey::SCAN_FOR_INSERT ||
                                        scanKey.get_request_type() == TwoPLPashaRWKey::SCAN_FOR_DELETE) {
                                        // release write locks
                                        if (partitioner.has_master_partition(partitionId)) {
                                                for (auto i = 0u; i < scan_results.size(); i++) {
                                                        std::atomic<uint64_t> *meta = scan_results[i].meta;
                                                        DCHECK(meta != nullptr);
                                                        TwoPLPashaHelper::write_lock_release(*meta);
                                                }
                                        } else {
                                                for (auto i = 0u; i < scan_results.size(); i++) {
                                                        char *cxl_row = reinterpret_cast<char *>(scan_results[i].meta);
                                                        DCHECK(cxl_row != nullptr);
                                                        TwoPLPashaHelper::remote_write_lock_release(cxl_row);
                                                }
                                        }
                                } else {
                                        DCHECK(0);
                                }
                        }
                } else {
                        // rollback inserts
                        auto &insertSet = txn.insertSet;

                        for (auto i = 0u; i < insertSet.size(); i++) {
                                auto &insertKey = insertSet[i];

                                if (insertKey.get_processed() == false)
                                        continue;

                                auto tableId = insertKey.get_table_id();
                                auto partitionId = insertKey.get_partition_id();
                                auto table = db.find_table(tableId, partitionId);
                                if (partitioner.has_master_partition(partitionId)) {
                                        // remove the placeholder
                                        auto key = insertKey.get_key();
                                        bool success = table->remove(key);
                                        DCHECK(success == true);
                                } else {
                                        // currently the execution logic should never reach here
                                        DCHECK(0);
                                }
                        }

                        // rollback deletes - just release the write locks
                        auto &deleteSet = txn.deleteSet;

                        for (auto i = 0u; i < deleteSet.size(); i++) {
                                auto &deleteKey = deleteSet[i];

                                if (deleteKey.get_processed() == false)
                                        continue;

                                auto tableId = deleteKey.get_table_id();
                                auto partitionId = deleteKey.get_partition_id();
                                auto table = db.find_table(tableId, partitionId);
                                if (partitioner.has_master_partition(partitionId)) {
                                        auto key = deleteKey.get_key();
                                        auto row = table->search(key);
                                        std::atomic<uint64_t> &meta = *reinterpret_cast<std::atomic<uint64_t> *>(std::get<0>(row));
                                        TwoPLPashaHelper::write_lock_release(meta);
                                } else {
                                        // does not support remote insert & delete
                                        DCHECK(0);
                                }
                        }

                        // assume all writes are updates
                        auto &readSet = txn.readSet;

                        // release locks in the read/write set
                        for (auto i = 0u; i < readSet.size(); i++) {
                                // release read locks in the readSet
                                auto &readKey = readSet[i];
                                auto tableId = readKey.get_table_id();
                                auto partitionId = readKey.get_partition_id();
                                auto table = db.find_table(tableId, partitionId);
                                if (readKey.get_read_lock_bit()) {
                                        if (partitioner.has_master_partition(partitionId)) {
                                                auto cached_row = readKey.get_cached_local_row();
                                                DCHECK(std::get<0>(cached_row) != nullptr && std::get<1>(cached_row) != nullptr);

                                                // model CXL search overhead
                                                if (this->context.model_cxl_search_overhead == true) {
                                                        twopl_pasha_global_helper->model_cxl_search_overhead(cached_row, tableId, partitionId, readKey.get_key());
                                                }

                                                TwoPLPashaHelper::read_lock_release(*std::get<0>(cached_row));
                                        } else {
                                                char *migrated_row = readKey.get_cached_migrated_row();
                                                DCHECK(migrated_row != nullptr);
                                                TwoPLPashaHelper::remote_read_lock_release(migrated_row);
                                        }
                                }

                                // release write locks in the writeSet
                                if (readKey.get_write_lock_bit()) {
                                        if (partitioner.has_master_partition(partitionId)) {
                                                auto cached_row = readKey.get_cached_local_row();
                                                DCHECK(std::get<0>(cached_row) != nullptr && std::get<1>(cached_row) != nullptr);

                                                // model CXL search overhead
                                                if (this->context.model_cxl_search_overhead == true) {
                                                        twopl_pasha_global_helper->model_cxl_search_overhead(cached_row, tableId, partitionId, readKey.get_key());
                                                }

                                                TwoPLPashaHelper::write_lock_release(*std::get<0>(cached_row));
                                        } else {
                                                char *migrated_row = readKey.get_cached_migrated_row();
                                                DCHECK(migrated_row != nullptr);
                                                TwoPLPashaHelper::remote_write_lock_release(migrated_row);
                                        }
                                }
                        }
                }

                // release migrated rows
                release_migrated_rows(txn);

                // reactively move out data
                if (migration_manager->when_to_move_out == MigrationManager::Reactive) {
                        // send out data move out hints
                        for (auto remote_host_id : txn.remote_hosts_involved) {
                                txn.network_size += MessageFactoryType::new_data_move_out_hint_message(*messages[remote_host_id]);
                        }
                }
	}

	bool commit(TransactionType &txn, std::vector<std::unique_ptr<Message> > &messages)
	{
		if (txn.abort_lock) {
			abort(txn, messages);
			return false;
		}

                uint64_t cur_global_epoch = txn.get_logger()->get_global_epoch();

                {
			ScopedTimer t([&, this](uint64_t us) { txn.record_commit_prepare_time(us); });
			if (txn.get_logger()) {
				write_redo_logs_for_commit(txn, cur_global_epoch);
			} else {
				// do nothing if logging is disabled
			}
		}

		// all locks are acquired

		uint64_t commit_tid;
		{
			ScopedTimer t([&, this](uint64_t us) { txn.record_local_work_time(us); });
			// generate tid
			commit_tid = generate_tid(txn);
		}

		{
                        // don't write commit log record for read-only transactions
                        if (txn.writeSet.size() != 0 || txn.insertSet.size() != 0 || txn.deleteSet.size() != 0) {
                                ScopedTimer t([&, this](uint64_t us) { txn.record_commit_persistence_time(us); });
                                // Persist commit record
                                if (txn.get_logger()) {
                                        std::ostringstream ss;
                                        ss << commit_tid << true;
                                        auto output = ss.str();
                                        auto lsn = txn.get_logger()->write(output.c_str(), output.size(), true, txn.startTime);
                                        // txn.get_logger()->sync(lsn, [&](){ txn.remote_request_handler(); });
                                }
                        }
		}

                if (this->context.enable_phantom_detection == true) {
                        // commit inserts
                        auto &insertSet = txn.insertSet;
                        for (auto i = 0u; i < insertSet.size(); i++) {
                                auto &insertKey = insertSet[i];
                                DCHECK(insertKey.get_processed() == true);

                                auto tableId = insertKey.get_table_id();
                                auto partitionId = insertKey.get_partition_id();
                                auto table = db.find_table(tableId, partitionId);
                                if (partitioner.has_master_partition(partitionId)) {
                                        auto key = insertKey.get_key();

                                        auto key_info_updater = [&](const void *prev_key, void *prev_meta, void *prev_data, const void *cur_key, void *cur_meta, void *cur_data, const void *next_key, void *next_meta, void *next_data) {
                                                auto prev_lmeta = reinterpret_cast<TwoPLPashaMetadataLocal *>(prev_meta);
                                                auto cur_lmeta = reinterpret_cast<TwoPLPashaMetadataLocal *>(cur_meta);
                                                auto next_lmeta = reinterpret_cast<TwoPLPashaMetadataLocal *>(next_meta);

                                                // update the next-key information for the previous key
                                                if (prev_lmeta != nullptr) {
                                                        prev_lmeta->lock();
                                                        if (prev_lmeta->is_migrated == true) {
                                                                auto prev_smeta = reinterpret_cast<TwoPLPashaMetadataShared *>(prev_lmeta->migrated_row);

                                                                prev_smeta->lock();
                                                                prev_smeta->clear_next_key_real_bit();
                                                                prev_smeta->unlock();
                                                        }
                                                        prev_lmeta->unlock();
                                                }

                                                // update the prev-key information for the next key
                                                if (next_lmeta != nullptr) {
                                                        next_lmeta->lock();
                                                        if (next_lmeta->is_migrated == true) {
                                                                auto next_smeta = reinterpret_cast<TwoPLPashaMetadataShared *>(next_lmeta->migrated_row);

                                                                next_smeta->lock();
                                                                next_smeta->clear_prev_key_real_bit();
                                                                next_smeta->unlock();
                                                        }
                                                        next_lmeta->unlock();
                                                }
                                        };

                                        bool update_key_info_success = table->search_and_update_next_key_info(key, key_info_updater);
                                        DCHECK(update_key_info_success == true);

                                        // make the placeholder as valid
                                        std::atomic<uint64_t> *meta = table->search_metadata(key);      // cannot use cached row
                                        DCHECK(meta != nullptr);
                                        TwoPLPashaHelper::modify_tuple_valid_bit(*meta, true);
                                } else {
                                        auto coordinatorID = partitioner.master_coordinator(partitionId);
                                        txn.network_size += MessageFactoryType::new_remote_insert_message(
                                                *messages[coordinatorID], *table, insertKey.get_key(), insertKey.get_value(), txn.transaction_id, i);
                                        txn.pendingResponses++;
                                }
                        }

                        // wait for the remote host to insert a placeholder and migrate it
                        // then mark the placeholder as valid
                        sync_messages(txn);

                        // commit deletes
                        auto &scanSet = txn.scanSet;
                        for (auto i = 0u; i < scanSet.size(); i++) {
                                auto &scanKey = scanSet[i];
                                DCHECK(scanKey.get_processed() == true);

                                if (scanKey.get_request_type() != TwoPLPashaRWKey::SCAN_FOR_DELETE) {
                                        continue;
                                }

                                auto tableId = scanKey.get_table_id();
                                auto partitionId = scanKey.get_partition_id();
                                auto table = db.find_table(tableId, partitionId);
                                std::vector<ITable::row_entity> &scan_results = *reinterpret_cast<std::vector<ITable::row_entity> *>(scanKey.get_scan_res_vec());
                                DCHECK(scan_results.size() > 0);

                                if (partitioner.has_master_partition(partitionId)) {
                                        for (auto i = 0u; i < scan_results.size(); i++) {
                                                auto key = reinterpret_cast<const void *>(scan_results[i].key);

                                                // delete the key and untrack it if necessary
                                                migration_manager->delete_specific_row_and_move_out(table, key, true);
                                        }
                                } else {
                                        auto coordinatorID = partitioner.master_coordinator(partitionId);
                                        for (auto i = 0u; i < scan_results.size(); i++) {
                                                char *cxl_row = reinterpret_cast<char *>(scan_results[i].meta);
                                                DCHECK(cxl_row != nullptr);
                                                TwoPLPashaHelper::remote_modify_tuple_valid_bit(cxl_row, false);
                                                // TwoPLPashaHelper::decrease_reference_count_via_ptr(cxl_row);
                                                txn.network_size += MessageFactoryType::new_remote_delete_message(*messages[coordinatorID], *table, scan_results[i].key);
                                        }
                                }
                        }
                } else {
                        // commit inserts
                        auto &insertSet = txn.insertSet;
                        for (auto i = 0u; i < insertSet.size(); i++) {
                                auto &insertKey = insertSet[i];
                                DCHECK(insertKey.get_processed() == true);

                                auto tableId = insertKey.get_table_id();
                                auto partitionId = insertKey.get_partition_id();
                                auto table = db.find_table(tableId, partitionId);
                                if (partitioner.has_master_partition(partitionId)) {
                                        // make the placeholder as valid
                                        auto key = insertKey.get_key();
                                        std::atomic<uint64_t> *meta = table->search_metadata(key);      // cannot use cached row
                                        DCHECK(meta != nullptr);
                                        TwoPLPashaHelper::modify_tuple_valid_bit(*meta, true);
                                } else {
                                        // does not support remote insert & delete
                                        DCHECK(0);
                                }
                        }

                        // commit deletes
                        auto &deleteSet = txn.deleteSet;
                        for (auto i = 0u; i < deleteSet.size(); i++) {
                                auto &deleteKey = deleteSet[i];
                                DCHECK(deleteKey.get_processed() == true);

                                auto tableId = deleteKey.get_table_id();
                                auto partitionId = deleteKey.get_partition_id();
                                auto table = db.find_table(tableId, partitionId);
                                if (partitioner.has_master_partition(partitionId)) {
                                        auto key = deleteKey.get_key();
                                        bool success = table->remove(key);
                                        DCHECK(success == true);
                                } else {
                                        // does not support remote insert & delete
                                        DCHECK(0);
                                }
                        }
                }

		{
			ScopedTimer t([&, this](uint64_t us) { txn.record_commit_write_back_time(us); });
			// write and replicate
			write_and_replicate(txn, commit_tid, messages);
		}

		// release locks
		{
			ScopedTimer t([&, this](uint64_t us) { txn.record_commit_unlock_time(us); });
			// write and replicate
			release_lock(txn, commit_tid, messages, cur_global_epoch);
		}

                // release migrated rows
                release_migrated_rows(txn);

                // reactively move out data
                if (migration_manager->when_to_move_out == MigrationManager::Reactive) {
                        // send out data move out hints
                        for (auto remote_host_id : txn.remote_hosts_involved) {
                                txn.network_size += MessageFactoryType::new_data_move_out_hint_message(*messages[remote_host_id]);
                        }
                }

		return true;
	}

	void write_and_replicate(TransactionType &txn, uint64_t commit_tid, std::vector<std::unique_ptr<Message> > &messages)
	{
		auto &readSet = txn.readSet;
		auto &writeSet = txn.writeSet;
                auto &scanSet = txn.scanSet;

		auto logger = txn.get_logger();
		bool wrote_local_log = false;
		std::vector<bool> persist_commit_record(writeSet.size(), false);
		std::vector<bool> coordinator_covered(this->context.coordinator_num, false);
		std::vector<std::vector<bool> > persist_replication(writeSet.size(), std::vector<bool>(this->context.coordinator_num, false));
		std::vector<bool> coordinator_covered_for_replication(this->context.coordinator_num, false);

		if (txn.get_logger()) {
			// We set persist_commit_record[i] to true if it is the last write to the coordinator
			// We traverse backwards and set the sync flag for the first write whose coordinator_covered is not true
			bool has_other_node = false;
			for (auto i = (int)writeSet.size() - 1; i >= 0; i--) {
				auto &writeKey = writeSet[i];
				auto tableId = writeKey.get_table_id();
				auto partitionId = writeKey.get_partition_id();
				auto table = db.find_table(tableId, partitionId);
				auto key_size = table->key_size();
				auto field_size = table->field_size();
				if (partitioner.has_master_partition(partitionId))
					continue;

                                // replication not supported
                                DCHECK(false);

				has_other_node = true;
				auto coordinatorId = partitioner.master_coordinator(partitionId);
				if (coordinator_covered[coordinatorId] == false) {
					coordinator_covered[coordinatorId] = true;
					persist_commit_record[i] = true;
				}

				for (auto k = 0u; k < partitioner.total_coordinators(); ++k) {
					// k does not have this partition
					if (!partitioner.is_partition_replicated_on(partitionId, k)) {
						continue;
					}

					// already write
					if (k == partitioner.master_coordinator(partitionId)) {
						continue;
					}

					// remote replication
					if (k != txn.coordinator_id && coordinator_covered_for_replication[k] == false) {
						coordinator_covered_for_replication[k] = true;
						persist_replication[i][k] = true;
					}
				}
			}
			bool has_persist = false;
			for (size_t i = 0; i < writeSet.size(); ++i) {
				if (persist_commit_record[i]) {
					has_persist = true;
				}
			}
			if (writeSet.size() && has_other_node) {
				DCHECK(has_persist);
			}
		}

                for (auto i = 0u; i < readSet.size(); i++) {
                        if (readSet[i].get_write_lock_bit()) {
                                auto &writeKey = readSet[i];
                                auto tableId = writeKey.get_table_id();
                                auto partitionId = writeKey.get_partition_id();
                                auto table = db.find_table(tableId, partitionId);

                                if (partitioner.has_master_partition(partitionId)) {
                                        auto value = writeKey.get_value();
                                        auto value_size = table->value_size();
                                        auto cached_row = writeKey.get_cached_local_row();
                                        DCHECK(std::get<0>(cached_row) != nullptr && std::get<1>(cached_row) != nullptr);

                                        // model CXL search overhead
                                        if (this->context.model_cxl_search_overhead == true) {
                                                twopl_pasha_global_helper->model_cxl_search_overhead(cached_row, tableId, partitionId, writeKey.get_key());
                                        }

                                        twopl_pasha_global_helper->update(cached_row, value, value_size);
                                } else {
                                        auto key = writeKey.get_key();
                                        auto value = writeKey.get_value();
                                        auto value_size = table->value_size();
                                        char *migrated_row = writeKey.get_cached_migrated_row();
                                        DCHECK(migrated_row != nullptr);
                                        twopl_pasha_global_helper->remote_update(migrated_row, value, value_size);
                                }
                        }
                }

                if (this->context.enable_phantom_detection == true) {
                        // apply writes for "scan for update"
                        for (auto i = 0u; i < scanSet.size(); i++) {
                                auto &scanKey = scanSet[i];
                                auto tableId = scanKey.get_table_id();
                                auto partitionId = scanKey.get_partition_id();
                                auto table = db.find_table(tableId, partitionId);
                                std::vector<ITable::row_entity> &scan_results = *reinterpret_cast<std::vector<ITable::row_entity> *>(scanKey.get_scan_res_vec());

                                // write
                                if (scanKey.get_request_type() == TwoPLPashaRWKey::SCAN_FOR_UPDATE) {
                                        if (partitioner.has_master_partition(partitionId)) {
                                                for (auto i = 0u; i < scan_results.size(); i++) {
                                                        auto key = reinterpret_cast<const void *>(scan_results[i].key);
                                                        auto value = scan_results[i].data;
                                                        auto value_size = table->value_size();
                                                        std::tuple<MetaDataType *, void *> row = std::make_tuple(scan_results[i].meta, scan_results[i].data);
                                                        DCHECK(std::get<0>(row) != nullptr && std::get<1>(row) != nullptr);
                                                        twopl_pasha_global_helper->update(row, value, value_size);
                                                }
                                        } else {
                                                for (auto i = 0u; i < scan_results.size(); i++) {
                                                        char *cxl_row = reinterpret_cast<char *>(scan_results[i].meta);
                                                        TwoPLPashaMetadataShared *smeta = reinterpret_cast<TwoPLPashaMetadataShared *>(cxl_row);
                                                        auto value_size = table->value_size();
                                                        twopl_pasha_global_helper->remote_update(cxl_row, smeta->get_scc_data()->data, value_size);
                                                }
                                        }
                                }
                        }
                } else {
                        // do nothing
                }
	}

        uint64_t generate_epoch_version(uint64_t cur_epoch_version, uint64_t cur_global_epoch)
        {
                uint64_t epoch = cur_epoch_version >> 32;
                if (cur_global_epoch > epoch) {
                        return cur_global_epoch << 32;
                } else {
                        return cur_epoch_version + 1;
                }
        }

        void write_redo_logs_for_commit(TransactionType &txn, uint64_t cur_global_epoch)
	{
                // Redo logging for updates
                auto &writeSet = txn.writeSet;
                for (size_t i = 0; i < writeSet.size(); ++i) {
                        auto &writeKey = writeSet[i];
                        auto tableId = writeKey.get_table_id();
                        auto partitionId = writeKey.get_partition_id();
                        auto table = db.find_table(tableId, partitionId);
                        auto key_size = table->key_size();
                        auto value_size = table->value_size();
                        auto key = writeKey.get_key();
                        auto value = writeKey.get_value();
                        auto tid = writeKey.get_tid();
                        DCHECK(key);
                        DCHECK(value);
                        std::ostringstream ss;

                        int log_type = 0;       // 0 stands for update
                        uint64_t epoch_version = generate_epoch_version(tid, cur_global_epoch);
                        ss << log_type << tableId << partitionId << epoch_version << std::string((char *)key, key_size) << std::string((char *)value, value_size);
                        auto output = ss.str();
                        txn.get_logger()->write(output.c_str(), output.size(), false, txn.startTime);
                }

                // TODO: write log records for scan_for_update

                // Redo logging for inserts
                auto &insertSet = txn.insertSet;
                for (size_t i = 0; i < insertSet.size(); ++i) {
                        auto &insertKey = insertSet[i];
                        auto tableId = insertKey.get_table_id();
                        auto partitionId = insertKey.get_partition_id();
                        auto table = db.find_table(tableId, partitionId);
                        auto key_size = table->key_size();
                        auto value_size = table->value_size();
                        auto key = insertKey.get_key();
                        auto value = insertKey.get_value();
                        auto tid = insertKey.get_tid();
                        DCHECK(key);
                        DCHECK(value);
                        std::ostringstream ss;

                        int log_type = 1;       // 1 stands for insert
                        uint64_t epoch_version = generate_epoch_version(tid, cur_global_epoch);
                        ss << log_type << tableId << partitionId << epoch_version << std::string((char *)key, key_size) << std::string((char *)value, value_size);
                        auto output = ss.str();
                        txn.get_logger()->write(output.c_str(), output.size(), false, txn.startTime);
                }

                // Redo logging for deletes
                auto &deleteSet = txn.deleteSet;
                for (size_t i = 0; i < deleteSet.size(); ++i) {
                        auto &deleteKey = deleteSet[i];
                        auto tableId = deleteKey.get_table_id();
                        auto partitionId = deleteKey.get_partition_id();
                        auto table = db.find_table(tableId, partitionId);
                        auto key_size = table->key_size();
                        auto value_size = table->value_size();
                        auto key = deleteKey.get_key();
                        auto value = deleteKey.get_value();
                        auto tid = deleteKey.get_tid();
                        DCHECK(key);
                        DCHECK(value);
                        std::ostringstream ss;

                        int log_type = 2;       // 2 stands for delete
                        uint64_t epoch_version = generate_epoch_version(tid, cur_global_epoch);
                        ss << log_type << tableId << partitionId << epoch_version << std::string((char *)key, key_size);     // do not need to log value for deletes
                        auto output = ss.str();
                        txn.get_logger()->write(output.c_str(), output.size(), false, txn.startTime);
                }
	}

	void release_lock(TransactionType &txn, uint64_t commit_tid, std::vector<std::unique_ptr<Message> > &messages, uint64_t cur_global_epoch)
	{
		// release read locks & write locks
		auto &readSet = txn.readSet;

		for (auto i = 0u; i < readSet.size(); i++) {
			auto &readKey = readSet[i];
			auto tableId = readKey.get_table_id();
			auto partitionId = readKey.get_partition_id();
			auto table = db.find_table(tableId, partitionId);
			if (readKey.get_read_lock_bit()) {
				if (partitioner.has_master_partition(partitionId)) {
					auto cached_row = readKey.get_cached_local_row();
                                        DCHECK(std::get<0>(cached_row) != nullptr && std::get<1>(cached_row) != nullptr);

                                        // model CXL search overhead
                                        if (this->context.model_cxl_search_overhead == true) {
                                                twopl_pasha_global_helper->model_cxl_search_overhead(cached_row, tableId, partitionId, readKey.get_key());
                                        }

					TwoPLPashaHelper::read_lock_release(*std::get<0>(cached_row));
				} else {
					char *migrated_row = readKey.get_cached_migrated_row();
                                        DCHECK(migrated_row != nullptr);
                                        twopl_pasha_global_helper->remote_read_lock_release(migrated_row);
				}
			}

                        if (readKey.get_write_lock_bit()) {
				if (partitioner.has_master_partition(partitionId)) {
                                        auto cached_row = readKey.get_cached_local_row();
                                        auto tid = readKey.get_tid();
                                        DCHECK(std::get<0>(cached_row) != nullptr && std::get<1>(cached_row) != nullptr);

                                        // model CXL search overhead
                                        if (this->context.model_cxl_search_overhead == true) {
                                                twopl_pasha_global_helper->model_cxl_search_overhead(cached_row, tableId, partitionId, readKey.get_key());
                                        }

                                        uint64_t epoch_version = generate_epoch_version(tid, cur_global_epoch);
                                        twopl_pasha_global_helper->write_lock_release(*std::get<0>(cached_row), table->value_size(), epoch_version);
                                } else {
                                        char *migrated_row = readKey.get_cached_migrated_row();
                                        auto tid = readKey.get_tid();
                                        DCHECK(migrated_row != nullptr);

                                        uint64_t epoch_version = generate_epoch_version(tid, cur_global_epoch);
                                        twopl_pasha_global_helper->remote_write_lock_release(migrated_row, table->value_size(), epoch_version);
                                }
			}
		}

                if (this->context.enable_phantom_detection == true) {
                        // release write locks for the next keys of inserts
                        auto &insertSet = txn.insertSet;

                        for (auto i = 0u; i < insertSet.size(); i++) {
                                auto &insertKey = insertSet[i];
                                auto tableId = insertKey.get_table_id();
                                auto partitionId = insertKey.get_partition_id();
                                auto table = db.find_table(tableId, partitionId);

                                // release the next row
                                if (insertKey.get_next_row_locked() == true) {
                                        if (partitioner.has_master_partition(partitionId)) {
                                                auto next_row_entity = insertKey.get_next_row_entity();
                                                std::atomic<uint64_t> *meta = next_row_entity.meta;
                                                DCHECK(meta != nullptr);
                                                twopl_pasha_global_helper->write_lock_release(*meta, table->value_size(), commit_tid);
                                        } else {
                                                auto next_row_entity = insertKey.get_next_row_entity();
                                                char *cxl_row = reinterpret_cast<char *>(next_row_entity.meta);
                                                DCHECK(cxl_row != nullptr);
                                                twopl_pasha_global_helper->remote_write_lock_release(cxl_row, table->value_size(), commit_tid);
                                        }
                                }
                        }

                        // release locks in the scan set
                        auto &scanSet = txn.scanSet;

                        for (auto i = 0u; i < scanSet.size(); i++) {
                                // release read locks in the scanSet
                                auto &scanKey = scanSet[i];
                                auto tableId = scanKey.get_table_id();
                                auto partitionId = scanKey.get_partition_id();
                                auto table = db.find_table(tableId, partitionId);
                                std::vector<ITable::row_entity> &scan_results = *reinterpret_cast<std::vector<ITable::row_entity> *>(scanKey.get_scan_res_vec());

                                // release the next row
                                DCHECK(scanKey.get_next_row_locked() == true);
                                if (scanKey.get_next_row_locked() == true) {
                                        if (partitioner.has_master_partition(partitionId)) {
                                                auto next_row_entity = scanKey.get_next_row_entity();
                                                std::atomic<uint64_t> *meta = next_row_entity.meta;
                                                DCHECK(meta != nullptr);
                                                if (scanKey.get_request_type() == TwoPLPashaRWKey::SCAN_FOR_READ) {
                                                        TwoPLPashaHelper::read_lock_release(*meta);
                                                } else if (scanKey.get_request_type() == TwoPLPashaRWKey::SCAN_FOR_UPDATE) {
                                                        TwoPLPashaHelper::write_lock_release(*meta);
                                                } else if (scanKey.get_request_type() == TwoPLPashaRWKey::SCAN_FOR_INSERT) {
                                                        TwoPLPashaHelper::write_lock_release(*meta);
                                                } else if (scanKey.get_request_type() == TwoPLPashaRWKey::SCAN_FOR_DELETE) {
                                                        TwoPLPashaHelper::write_lock_release(*meta);
                                                } else {
                                                        DCHECK(0);
                                                }
                                        } else {
                                                DCHECK(scanKey.get_next_row_locked() == true);
                                                auto next_row_entity = scanKey.get_next_row_entity();
                                                char *cxl_row = reinterpret_cast<char *>(next_row_entity.meta);
                                                DCHECK(cxl_row != nullptr);
                                                if (scanKey.get_request_type() == TwoPLPashaRWKey::SCAN_FOR_READ) {
                                                        TwoPLPashaHelper::remote_read_lock_release(cxl_row);
                                                } else if (scanKey.get_request_type() == TwoPLPashaRWKey::SCAN_FOR_UPDATE) {
                                                        TwoPLPashaHelper::remote_write_lock_release(cxl_row);
                                                } else if (scanKey.get_request_type() == TwoPLPashaRWKey::SCAN_FOR_INSERT) {
                                                        TwoPLPashaHelper::remote_write_lock_release(cxl_row);
                                                } else if (scanKey.get_request_type() == TwoPLPashaRWKey::SCAN_FOR_DELETE) {
                                                        TwoPLPashaHelper::remote_write_lock_release(cxl_row);
                                                } else {
                                                        DCHECK(0);
                                                }
                                        }
                                }

                                if (scanKey.get_request_type() == TwoPLPashaRWKey::SCAN_FOR_READ) {
                                        // release read locks
                                        if (partitioner.has_master_partition(partitionId)) {
                                                for (auto i = 0u; i < scan_results.size(); i++) {
                                                        auto key = reinterpret_cast<const void *>(scan_results[i].key);
                                                        std::atomic<uint64_t> *meta = scan_results[i].meta;
                                                        DCHECK(meta != nullptr);
                                                        TwoPLPashaHelper::read_lock_release(*meta);
                                                }
                                        } else {
                                                for (auto i = 0u; i < scan_results.size(); i++) {
                                                        char *cxl_row = reinterpret_cast<char *>(scan_results[i].meta);
                                                        DCHECK(cxl_row != nullptr);
                                                        TwoPLPashaHelper::remote_read_lock_release(cxl_row);
                                                }
                                        }
                                } else if (scanKey.get_request_type() == TwoPLPashaRWKey::SCAN_FOR_UPDATE) {
                                        // release write locks for updates
                                        if (partitioner.has_master_partition(partitionId)) {
                                                for (auto i = 0u; i < scan_results.size(); i++) {
                                                        auto key = reinterpret_cast<const void *>(scan_results[i].key);
                                                        std::atomic<uint64_t> *meta = scan_results[i].meta;
                                                        DCHECK(meta != nullptr);
                                                        // TODO: update epoch-version
                                                        TwoPLPashaHelper::write_lock_release(*meta);
                                                }
                                        } else {
                                                for (auto i = 0u; i < scan_results.size(); i++) {
                                                        char *cxl_row = reinterpret_cast<char *>(scan_results[i].meta);
                                                        DCHECK(cxl_row != nullptr);
                                                        // TODO: update epoch-version
                                                        TwoPLPashaHelper::remote_write_lock_release(cxl_row);
                                                }
                                        }
                                } else if (scanKey.get_request_type() == TwoPLPashaRWKey::SCAN_FOR_INSERT) {
                                        // release write locks for updates
                                        if (partitioner.has_master_partition(partitionId)) {
                                                for (auto i = 0u; i < scan_results.size(); i++) {
                                                        auto key = reinterpret_cast<const void *>(scan_results[i].key);
                                                        std::atomic<uint64_t> *meta = scan_results[i].meta;
                                                        DCHECK(meta != nullptr);
                                                        // TODO: update epoch-version
                                                        TwoPLPashaHelper::write_lock_release(*meta);
                                                }
                                        } else {
                                                for (auto i = 0u; i < scan_results.size(); i++) {
                                                        char *cxl_row = reinterpret_cast<char *>(scan_results[i].meta);
                                                        DCHECK(cxl_row != nullptr);
                                                        // TODO: update epoch-version
                                                        TwoPLPashaHelper::remote_write_lock_release(cxl_row);
                                                }
                                        }
                                } else if (scanKey.get_request_type() == TwoPLPashaRWKey::SCAN_FOR_DELETE) {
                                        // no need to release write locks for tuples to be deleted
                                        // because they are already removed!
                                } else {
                                        DCHECK(0);
                                }
                        }
                } else {
                        // do nothing
                }
	}

        void release_migrated_rows(TransactionType &txn)
        {
                // release rows in the read set
                auto &readSet = txn.readSet;
		for (auto i = 0u; i < readSet.size(); ++i) {
			auto &readKey = readSet[i];
                        auto tableId = readKey.get_table_id();
			auto partitionId = readKey.get_partition_id();
			auto table = db.find_table(tableId, partitionId);

			if (readKey.get_reference_counted() == true) {
                                DCHECK(partitioner.has_master_partition(partitionId) == false);
                                auto key = readKey.get_key();
                                twopl_pasha_global_helper->release_migrated_row(tableId, partitionId, key);
                        }
		}

                // release rows in the insert set
                auto &insertSet = txn.insertSet;
		for (auto i = 0u; i < insertSet.size(); ++i) {
			auto &insertKey = insertSet[i];
                        auto tableId = insertKey.get_table_id();
			auto partitionId = insertKey.get_partition_id();
			auto table = db.find_table(tableId, partitionId);

			if (insertKey.get_reference_counted() == true) {
                                if (partitioner.has_master_partition(partitionId)) {
                                        auto key = insertKey.get_key();
                                        twopl_pasha_global_helper->release_migrated_row(tableId, partitionId, key);
                                } else {
                                        char *inserted_cxl_row = insertKey.get_inserted_cxl_row();
                                        DCHECK(inserted_cxl_row != nullptr);
                                        TwoPLPashaHelper::decrease_reference_count_via_ptr(inserted_cxl_row);
                                }
                        }
		}

                if (this->context.enable_phantom_detection == true) {
                        // release rows in the scan set
                        auto &scanSet = txn.scanSet;
                        for (auto i = 0u; i < scanSet.size(); i++) {
                                // release read locks in the scanSet
                                auto &scanKey = scanSet[i];
                                auto tableId = scanKey.get_table_id();
                                auto partitionId = scanKey.get_partition_id();
                                auto table = db.find_table(tableId, partitionId);
                                std::vector<ITable::row_entity> &scan_results = *reinterpret_cast<std::vector<ITable::row_entity> *>(scanKey.get_scan_res_vec());

                                if (partitioner.has_master_partition(partitionId) == false) {
                                        if (scanKey.get_request_type() == TwoPLPashaRWKey::SCAN_FOR_DELETE) {
                                                // release the next row
                                                if (scanKey.get_next_row_locked() == true) {
                                                        auto next_row_entity = scanKey.get_next_row_entity();
                                                        char *cxl_row = reinterpret_cast<char *>(next_row_entity.meta);
                                                        DCHECK(cxl_row != nullptr);
                                                        TwoPLPashaHelper::decrease_reference_count_via_ptr(cxl_row);
                                                }

                                                // no need to release the row to be deleted
                                        } else {
                                                // release the next row
                                                if (scanKey.get_next_row_locked() == true) {
                                                        auto next_row_entity = scanKey.get_next_row_entity();
                                                        char *cxl_row = reinterpret_cast<char *>(next_row_entity.meta);
                                                        DCHECK(cxl_row != nullptr);
                                                        TwoPLPashaHelper::decrease_reference_count_via_ptr(cxl_row);
                                                }

                                                for (auto i = 0u; i < scan_results.size(); i++) {
                                                        char *cxl_row = reinterpret_cast<char *>(scan_results[i].meta);
                                                        DCHECK(cxl_row != nullptr);
                                                        TwoPLPashaHelper::decrease_reference_count_via_ptr(cxl_row);
                                                }
                                        }
                                }
                        }
                } else {
                        // do nothing
                }
        }

	void sync_messages(TransactionType &txn, bool wait_response = true)
	{
		txn.message_flusher();
		if (wait_response) {
			while (txn.pendingResponses > 0) {
				txn.remote_request_handler(0);
			}
		}
	}

    private:
	DatabaseType &db;
	const ContextType &context;
	Partitioner &partitioner;
	uint64_t max_tid = 0;
};
} // namespace star

//
// Created by Yibo Huang on 8/13/24.
//

#pragma once

#include <atomic>
#include <list>
#include <tuple>
#include <memory>

#include "common/CCSet.h"
#include "common/CCHashTable.h"
#include "core/CXLTable.h"
#include "core/Table.h"
#include "glog/logging.h"

#include "protocol/Pasha/MigrationManager.h"
#include "protocol/Pasha/SCCManager.h"

#include <boost/interprocess/offset_ptr.hpp>

namespace star
{

struct SundialPashaMetadataLocal {
        SundialPashaMetadataLocal()
                : wts(0)
                , rts(0)
                , owner(0)
                , is_migrated(false)
                , migrated_row(nullptr)
                , is_valid(false)
        {
                // this spinlock will only be shared within a single process
                pthread_spin_init(&latch, PTHREAD_PROCESS_PRIVATE);
        }

	void lock()
	{
                pthread_spin_lock(&latch);
	}

	void unlock()
	{
		pthread_spin_unlock(&latch);
	}

        pthread_spinlock_t latch;

	uint64_t wts{ 0 };
	uint64_t rts{ 0 };
	uint64_t owner{ 0 };

        bool is_migrated{ false };
        char *migrated_row{ nullptr };

        bool is_valid{ false };
};

struct SundialPashaMetadataShared {
	SundialPashaMetadataShared()
                : wts(0)
                , rts(0)
                , owner(0)
                , ref_cnt(0)
                , is_valid(false)
                , scc_meta(0)
                , is_next_key_real(false)
        {
                // this spinlock will be shared between multiple processes
                pthread_spin_init(&latch, PTHREAD_PROCESS_SHARED);
        }

	void lock()
	{
                pthread_spin_lock(&latch);
	}

	void unlock()
	{
		pthread_spin_unlock(&latch);
	}

        pthread_spinlock_t latch;

	uint64_t wts{ 0 };
	uint64_t rts{ 0 };
	uint64_t owner{ 0 };

        // multi-host transaction accessing a cxl row would increase its reference count by 1
        // a migrated row can only be moved out if its ref_cnt == 0
        uint64_t ref_cnt{ 0 };

        // a migrated tuple can be invalid if it is deleted or migrated out
        bool is_valid{ false };

        // migration policy metadata
        char migration_policy_meta[MigrationManager::migration_policy_meta_size];         // directly embed it here to avoid extra cxlalloc_malloc

        // software cache-coherence metadata
        uint64_t scc_meta{ 0 };         // directly embed it here to avoid extra cxlalloc_malloc

        // next-key information
        bool is_next_key_real{ false };
};

uint64_t SundialPashaMetadataLocalInit(bool is_tuple_valid);

/* 
 * lmeta means local metadata stored in local DRAM
 * smeta means shared metadata stored in the shared region
 */
class SundialPashaHelper {
    public:
        using MetaDataType = std::atomic<uint64_t>;

        SundialPashaHelper(std::size_t coordinator_id, std::vector<std::vector<CXLTableBase *> > &cxl_tbl_vecs)
                : coordinator_id(coordinator_id)
                , cxl_tbl_vecs(cxl_tbl_vecs)
        {
        }

	// Returns <rts, wts> of the tuple.
	std::pair<uint64_t, uint64_t> read(const std::tuple<MetaDataType *, void *> &row, void *dest, std::size_t size, std::atomic<uint64_t> &local_cxl_access)
	{
		MetaDataType &meta = *std::get<0>(row);
		SundialPashaMetadataLocal *lmeta = reinterpret_cast<SundialPashaMetadataLocal *>(meta.load());
                uint64_t rts = 0, wts = 0;

		lmeta->lock();
                if (lmeta->is_migrated == false) {
                        void *src = std::get<1>(row);
                        rts = lmeta->rts;
                        wts = lmeta->wts;
                        std::memcpy(dest, src, size);
                } else {
                        // statistics
                        local_cxl_access.fetch_add(1);

                        SundialPashaMetadataShared *smeta = reinterpret_cast<SundialPashaMetadataShared *>(lmeta->migrated_row);
                        void *src = lmeta->migrated_row + sizeof(SundialPashaMetadataShared);
                        smeta->lock();
                        CHECK(smeta->is_valid == true);
                        rts = smeta->rts;
                        wts = smeta->wts;
                        scc_manager->do_read(&smeta->scc_meta, coordinator_id, dest, src, size);
                        smeta->unlock();
                }
		lmeta->unlock();

		return std::make_pair(wts, rts);
	}

        // Returns <rts, wts> of the tuple.
	std::pair<uint64_t, uint64_t> remote_read(char *row, void *dest, std::size_t size)
	{
		SundialPashaMetadataShared *smeta = reinterpret_cast<SundialPashaMetadataShared *>(row);
                void *src = row + sizeof(SundialPashaMetadataShared);
                uint64_t rts = 0, wts = 0;

		smeta->lock();
                CHECK(smeta->is_valid == true);
                CHECK(smeta->ref_cnt > 0);
                rts = smeta->rts;
                wts = smeta->wts;
                scc_manager->do_read(&smeta->scc_meta, coordinator_id, dest, src, size);
		smeta->unlock();

		return std::make_pair(wts, rts);
	}

	// Returns <rts, wts> of the tuple.
	static bool write_lock(const std::tuple<MetaDataType *, void *> &row, std::pair<uint64_t, uint64_t> &rwts, uint64_t transaction_id)
	{
		MetaDataType &meta = *std::get<0>(row);
		SundialPashaMetadataLocal *lmeta = reinterpret_cast<SundialPashaMetadataLocal *>(meta.load());
		bool success = false;

		lmeta->lock();
                if (lmeta->is_valid == false) {
                        goto out_lmeta_unlock;
                }
                if (lmeta->is_migrated == false) {
                        rwts.first = lmeta->wts;
                        rwts.second = lmeta->rts;
                        if (lmeta->owner == 0 || lmeta->owner == transaction_id) {
                                success = true;
                                lmeta->owner = transaction_id;
                        }
                } else {
                        SundialPashaMetadataShared *smeta = reinterpret_cast<SundialPashaMetadataShared *>(lmeta->migrated_row);
                        smeta->lock();
                        CHECK(smeta->is_valid == true);
                        rwts.first = smeta->wts;
                        rwts.second = smeta->rts;
                        if (smeta->owner == 0 || smeta->owner == transaction_id) {
                                success = true;
                                smeta->owner = transaction_id;
                        }
                        smeta->unlock();
                }

out_lmeta_unlock:
		lmeta->unlock();
		return success;
	}

        // Returns <rts, wts> of the tuple.
	static bool remote_write_lock(char *row, std::pair<uint64_t, uint64_t> &rwts, uint64_t transaction_id)
	{
		SundialPashaMetadataShared *smeta = reinterpret_cast<SundialPashaMetadataShared *>(row);
                bool success = false;

                smeta->lock();
                CHECK(smeta->is_valid == true);
                CHECK(smeta->ref_cnt > 0);
                rwts.first = smeta->wts;
                rwts.second = smeta->rts;
                if (smeta->owner == 0 || smeta->owner == transaction_id) {
                        success = true;
                        smeta->owner = transaction_id;
                }
                smeta->unlock();

		return success;
	}

	static bool renew_lease(const std::tuple<MetaDataType *, void *> &row, uint64_t wts, uint64_t commit_ts)
	{
		MetaDataType &meta = *std::get<0>(row);
		SundialPashaMetadataLocal *lmeta = reinterpret_cast<SundialPashaMetadataLocal *>(meta.load());
		bool success = false;

		lmeta->lock();
                if (lmeta->is_valid == false) {
                        goto out_lmeta_unlock;
                }
                if (lmeta->is_migrated == false) {
                        if (wts != lmeta->wts || (commit_ts > lmeta->rts && lmeta->owner != 0)) {
                                success = false;
                        } else {
                                success = true;
                                lmeta->rts = std::max(lmeta->rts, commit_ts);
                        }
                } else {
                        SundialPashaMetadataShared *smeta = reinterpret_cast<SundialPashaMetadataShared *>(lmeta->migrated_row);
                        smeta->lock();
                        CHECK(smeta->is_valid == true);
                        if (wts != smeta->wts || (commit_ts > smeta->rts && smeta->owner != 0)) {
                                success = false;
                        } else {
                                success = true;
                                smeta->rts = std::max(smeta->rts, commit_ts);
                        }
                        smeta->unlock();
                }

out_lmeta_unlock:
		lmeta->unlock();
		return success;
	}

        static bool remote_renew_lease(char *row, uint64_t wts, uint64_t commit_ts)
	{
		SundialPashaMetadataShared *smeta = reinterpret_cast<SundialPashaMetadataShared *>(row);
                bool success = false;

                smeta->lock();
                CHECK(smeta->is_valid == true);
                CHECK(smeta->ref_cnt > 0);
                if (wts != smeta->wts || (commit_ts > smeta->rts && smeta->owner != 0)) {
                        success = false;
                } else {
                        success = true;
                        smeta->rts = std::max(smeta->rts, commit_ts);
                }
                smeta->unlock();

		return success;
	}

	static void replica_update(const std::tuple<MetaDataType *, void *> &row, const void *value, std::size_t value_size, uint64_t commit_ts)
	{
		CHECK(0);
	}

        static void remote_replica_update(char *row, const void *value, std::size_t value_size, uint64_t commit_ts)
	{
		CHECK(0);
	}

	void update(const std::tuple<MetaDataType *, void *> &row, const void *value, std::size_t value_size, uint64_t commit_ts,
			   uint64_t transaction_id)
	{
		MetaDataType &meta = *std::get<0>(row);
		SundialPashaMetadataLocal *lmeta = reinterpret_cast<SundialPashaMetadataLocal *>(meta.load());

		lmeta->lock();
                CHECK(lmeta->is_valid == true);
                if (lmeta->is_migrated == false) {
                        void *data_ptr = std::get<1>(row);
                        CHECK(lmeta->owner == transaction_id);
                        std::memcpy(data_ptr, value, value_size);
                        lmeta->wts = lmeta->rts = commit_ts;
                } else {
                        SundialPashaMetadataShared *smeta = reinterpret_cast<SundialPashaMetadataShared *>(lmeta->migrated_row);
                        void *data_ptr = lmeta->migrated_row + sizeof(SundialPashaMetadataShared);
                        smeta->lock();
                        CHECK(smeta->is_valid == true);
                        CHECK(smeta->owner == transaction_id);
                        scc_manager->do_write(&smeta->scc_meta, coordinator_id, data_ptr, value, value_size);
                        smeta->wts = smeta->rts = commit_ts;
                        smeta->unlock();
                }
		lmeta->unlock();
	}

        void remote_update(char *row, const void *value, std::size_t value_size, uint64_t commit_ts,
			   uint64_t transaction_id)
	{
		SundialPashaMetadataShared *smeta = reinterpret_cast<SundialPashaMetadataShared *>(row);
                void *data_ptr = row + sizeof(SundialPashaMetadataShared);

                smeta->lock();
                CHECK(smeta->is_valid == true);
                CHECK(smeta->ref_cnt > 0);
                CHECK(smeta->owner == transaction_id);
                scc_manager->do_write(&smeta->scc_meta, coordinator_id, data_ptr, value, value_size);
                smeta->wts = smeta->rts = commit_ts;
                smeta->unlock();
	}

	static void unlock(const std::tuple<MetaDataType *, void *> &row, uint64_t transaction_id)
	{
		MetaDataType &meta = *std::get<0>(row);
		SundialPashaMetadataLocal *lmeta = reinterpret_cast<SundialPashaMetadataLocal *>(meta.load());

		lmeta->lock();
                CHECK(lmeta->is_valid == true);
                if (lmeta->is_migrated == false) {
                        CHECK(lmeta->owner == transaction_id);
		        lmeta->owner = 0;
                } else {
                        SundialPashaMetadataShared *smeta = reinterpret_cast<SundialPashaMetadataShared *>(lmeta->migrated_row);
                        smeta->lock();
                        CHECK(smeta->is_valid == true);
                        CHECK(smeta->owner == transaction_id);
		        smeta->owner = 0;
                        smeta->unlock();
                }
		lmeta->unlock();
	}

        static void remote_unlock(char *row, uint64_t transaction_id)
	{
		SundialPashaMetadataShared *smeta = reinterpret_cast<SundialPashaMetadataShared *>(row);

                smeta->lock();
                CHECK(smeta->is_valid == true);
                CHECK(smeta->ref_cnt > 0);
                CHECK(smeta->owner == transaction_id);
                smeta->owner = 0;
                smeta->unlock();
	}

        static void mark_tuple_as_valid(const std::tuple<MetaDataType *, void *> &row)
        {
                MetaDataType &meta = *std::get<0>(row);
		SundialPashaMetadataLocal *lmeta = reinterpret_cast<SundialPashaMetadataLocal *>(meta.load());

                lmeta->lock();
                CHECK(lmeta->is_valid == false);
                if (lmeta->is_migrated == false) {
                        lmeta->is_valid = true;
                } else {
                        CHECK(0);
                }
                lmeta->unlock();
        }

        void commit_pasha_metadata_init()
        {
                init_finished.store(1, std::memory_order_release);
        }

        void wait_for_pasha_metadata_init()
        {
                while (init_finished.load(std::memory_order_acquire) == 0);
        }

        char *get_migrated_row(std::size_t table_id, std::size_t partition_id, const void *key, bool inc_ref_cnt)
        {
                CXLTableBase *target_cxl_table = cxl_tbl_vecs[table_id][partition_id];
                char *migrated_row = reinterpret_cast<char *>(target_cxl_table->search(key));
                void *migration_policy_meta = nullptr;

                if (migrated_row != nullptr) {
                        SundialPashaMetadataShared *smeta = reinterpret_cast<SundialPashaMetadataShared *>(migrated_row);
                        smeta->lock();
                        if (smeta->is_valid == true) {
                                if (inc_ref_cnt == true) {
                                        smeta->ref_cnt++;
                                        migration_policy_meta = &smeta->migration_policy_meta;
                                }
                        } else {
                                migrated_row = nullptr;
                        }
                        smeta->unlock();
                }

                if (migration_policy_meta != nullptr) {
                        migration_manager->access_row(migration_policy_meta, partition_id);
                }

                return migrated_row;
        }

        void release_migrated_row(std::size_t table_id, std::size_t partition_id, const void *key)
        {
                CXLTableBase *target_cxl_table = cxl_tbl_vecs[table_id][partition_id];
                char *migrated_row = reinterpret_cast<char *>(target_cxl_table->search(key));
                CHECK(migrated_row != nullptr);

                SundialPashaMetadataShared *smeta = reinterpret_cast<SundialPashaMetadataShared *>(migrated_row);
                smeta->lock();
                CHECK(smeta->is_valid == true);
                CHECK(smeta->ref_cnt > 0);
                smeta->ref_cnt--;
                smeta->unlock();
        }

        bool move_from_hashmap_to_shared_region(ITable *table, const void *key, const std::tuple<MetaDataType *, void *> &row, bool inc_ref_cnt, void *&migration_policy_meta)
	{
                MetaDataType &meta = *std::get<0>(row);
		SundialPashaMetadataLocal *lmeta = reinterpret_cast<SundialPashaMetadataLocal *>(meta.load());
                void *local_data = std::get<1>(row);
                bool move_in_success = false;
                bool ret = false;

		lmeta->lock();
                CHECK(lmeta->is_valid == true);
                if (lmeta->is_migrated == false) {
                        // allocate the CXL row
                        std::size_t row_total_size = sizeof(SundialPashaMetadataShared) + table->value_size();
                        char *migrated_row_ptr = reinterpret_cast<char *>(cxl_memory.cxlalloc_malloc_wrapper(row_total_size,
                                CXLMemory::DATA_ALLOCATION, sizeof(SundialPashaMetadataShared), table->value_size()));
                        char *migrated_row_value_ptr = migrated_row_ptr + sizeof(SundialPashaMetadataShared);
                        SundialPashaMetadataShared *smeta = reinterpret_cast<SundialPashaMetadataShared *>(migrated_row_ptr);
                        new(smeta) SundialPashaMetadataShared();

                        // init migration policy metadata
                        migration_manager->init_migration_policy_metadata(&smeta->migration_policy_meta, table, key, row);
                        migration_policy_meta = smeta->migration_policy_meta;

                        // init software cache-coherence metadata
                        scc_manager->init_scc_metadata(&smeta->scc_meta, coordinator_id);

                        // take the CXL latch
                        smeta->lock();

                        // copy metadata
                        smeta->wts = lmeta->wts;
                        smeta->rts = lmeta->rts;
                        smeta->owner = lmeta->owner;

                        // copy data
                        scc_manager->do_write(&smeta->scc_meta, coordinator_id, migrated_row_value_ptr, local_data, table->value_size());

                        // set the migrated row as valid
                        smeta->is_valid = true;

                        // increase the reference count for the requesting host
                        if (inc_ref_cnt == true) {
                                smeta->ref_cnt++;
                        }

                        // insert into the corresponding CXL table
                        CXLTableBase *target_cxl_table = cxl_tbl_vecs[table->tableID()][table->partitionID()];
                        ret = target_cxl_table->insert(key, migrated_row_ptr);
                        CHECK(ret == true);

                        // mark the local row as migrated
                        lmeta->migrated_row = migrated_row_ptr;
                        lmeta->is_migrated = true;

                        // release the CXL latch
                        smeta->unlock();

                        // LOG(INFO) << "moved in a row with key " << key << " from table " << table->tableID();

                        move_in_success = true;
                } else {
                        if (inc_ref_cnt == true) {
                                // increase the reference count for the requesting host, even if it is already migrated
                                SundialPashaMetadataShared *smeta = reinterpret_cast<SundialPashaMetadataShared *>(lmeta->migrated_row);
                                smeta->lock();
                                CHECK(smeta->is_valid == true);
                                smeta->ref_cnt++;
                                smeta->unlock();
                        }
                        move_in_success = false;
                }
		lmeta->unlock();

		return move_in_success;
	}

        bool move_from_btree_to_shared_region(ITable *table, const void *key, const std::tuple<MetaDataType *, void *> &row, bool inc_ref_cnt, void *&migration_policy_meta)
	{
                MetaDataType &meta = *std::get<0>(row);
		SundialPashaMetadataLocal *lmeta = reinterpret_cast<SundialPashaMetadataLocal *>(meta.load());
                void *local_data = std::get<1>(row);
                bool move_in_success = false;
                bool ret = false;

                auto move_in_processor = [&](const void *prev_key, void *prev_meta, void *prev_data, const void *cur_key, void *cur_meta, void *cur_data, const void *next_key, void *next_meta, void *next_data) {
                        auto prev_lmeta = reinterpret_cast<SundialPashaMetadataLocal *>(prev_meta);
                        auto cur_lmeta = reinterpret_cast<SundialPashaMetadataLocal *>(cur_meta);
                        auto next_lmeta = reinterpret_cast<SundialPashaMetadataLocal *>(next_meta);

                        CHECK(lmeta != nullptr && cur_lmeta == lmeta);

                        bool is_next_key_migrated = false;

                        // check if the next tuple is migrated
                        if (next_lmeta != nullptr) {
                                next_lmeta->lock();
                                if (next_lmeta->is_migrated == true) {
                                        is_next_key_migrated = true;
                                }
                                next_lmeta->unlock();
                        } else {
                                is_next_key_migrated = true;
                        }

                        // check if the current tuple is migrated
                        // if yes, do the migration and update the next-key information
                        lmeta->lock();
                        CHECK(lmeta->is_valid == true);
                        if (lmeta->is_migrated == false) {
                                // allocate the CXL row
                                std::size_t row_total_size = sizeof(SundialPashaMetadataShared) + table->value_size();
                                char *migrated_row_ptr = reinterpret_cast<char *>(cxl_memory.cxlalloc_malloc_wrapper(row_total_size,
                                                CXLMemory::DATA_ALLOCATION, sizeof(SundialPashaMetadataShared), table->value_size()));
                                char *migrated_row_value_ptr = migrated_row_ptr + sizeof(SundialPashaMetadataShared);
                                SundialPashaMetadataShared *smeta = reinterpret_cast<SundialPashaMetadataShared *>(migrated_row_ptr);
                                new(smeta) SundialPashaMetadataShared();

                                // init migration policy metadata
                                migration_manager->init_migration_policy_metadata(&smeta->migration_policy_meta, table, key, row);
                                migration_policy_meta = smeta->migration_policy_meta;

                                // init software cache-coherence metadata
                                scc_manager->init_scc_metadata(&smeta->scc_meta, coordinator_id);

                                // take the CXL latch
                                smeta->lock();

                                // copy metadata
                                smeta->wts = lmeta->wts;
                                smeta->rts = lmeta->rts;
                                smeta->owner = lmeta->owner;

                                // copy data
                                scc_manager->do_write(&smeta->scc_meta, coordinator_id, migrated_row_value_ptr, local_data, table->value_size());

                                // set the migrated row as valid
                                smeta->is_valid = true;

                                // increase the reference count for the requesting host
                                if (inc_ref_cnt == true) {
                                        smeta->ref_cnt++;
                                }

                                // update the next-key information
                                if (is_next_key_migrated == true) {
                                        smeta->is_next_key_real = true;
                                } else {
                                        smeta->is_next_key_real = false;
                                }

                                // insert into the corresponding CXL table
                                CXLTableBase *target_cxl_table = cxl_tbl_vecs[table->tableID()][table->partitionID()];
                                ret = target_cxl_table->insert(key, migrated_row_ptr);
                                CHECK(ret == true);

                                // mark the local row as migrated
                                lmeta->migrated_row = migrated_row_ptr;
                                lmeta->is_migrated = true;

                                // release the CXL latch
                                smeta->unlock();

                                move_in_success = true;
                        } else {
                                if (inc_ref_cnt == true) {
                                        // increase the reference count for the requesting host, even if it is already migrated
                                        SundialPashaMetadataShared *smeta = reinterpret_cast<SundialPashaMetadataShared *>(lmeta->migrated_row);
                                        smeta->lock();
                                        CHECK(smeta->is_valid == true);
                                        smeta->ref_cnt++;
                                        smeta->unlock();
                                }
                                move_in_success = false;
                        }
                        lmeta->unlock();

                        if (move_in_success == true) {
                                // update the next-key information for the previous tuple
                                if (prev_lmeta != nullptr) {
                                        prev_lmeta->lock();
                                        if (prev_lmeta->is_migrated == true) {
                                                auto prev_smeta = reinterpret_cast<SundialPashaMetadataShared *>(prev_lmeta->migrated_row);
                                                prev_smeta->lock();
                                                prev_smeta->is_next_key_real = true;
                                                prev_smeta->unlock();
                                        }
                                        prev_lmeta->unlock();
                                }
                        }
		};

                // update next-key information
                ret = table->search_and_update_next_key_info(key, move_in_processor);
                CHECK(ret == true);

		return move_in_success;
	}

        bool move_from_partition_to_shared_region(ITable *table, const void *key, const std::tuple<MetaDataType *, void *> &row, bool inc_ref_cnt, void *&migration_policy_meta)
	{
                bool move_in_success = false;

                if (table->tableType() == ITable::HASHMAP) {
                        move_in_success = move_from_hashmap_to_shared_region(table, key, row, inc_ref_cnt, migration_policy_meta);
                } else if (table->tableType() == ITable::BTREE) {
                        move_in_success = move_from_btree_to_shared_region(table, key, row, inc_ref_cnt, migration_policy_meta);
                } else {
                        CHECK(0);
                }

                // statistics
                if (move_in_success == true) {
                        // LOG(INFO) << "moved in a row with key " << table->get_plain_key(key) << " from table " << table->tableID();
                        num_data_move_in.fetch_add(1);
                }

		return move_in_success;
	}

        bool move_from_hashmap_to_partition(ITable *table, const void *key, const std::tuple<MetaDataType *, void *> &row)
	{
                MetaDataType &meta = *std::get<0>(row);
		SundialPashaMetadataLocal *lmeta = reinterpret_cast<SundialPashaMetadataLocal *>(meta.load());
                void *local_data = std::get<1>(row);
                bool ret = false;

                lmeta->lock();
                CHECK(lmeta->is_valid == true);
                if (lmeta->is_migrated == true) {
                        SundialPashaMetadataShared *smeta = reinterpret_cast<SundialPashaMetadataShared *>(lmeta->migrated_row);
                        char *migrated_row_value = lmeta->migrated_row + sizeof(SundialPashaMetadataShared);

                        // take the CXL latch
                        smeta->lock();

                        // the tuple must be valid
                        CHECK(smeta->is_valid == true);

                        // reference count > 0, cannot move out the tuple
                        if (smeta->ref_cnt > 0) {
                                smeta->unlock();
                                lmeta->unlock();
                                return false;
                        }

                        // copy metadata back
                        lmeta->wts = smeta->wts;
                        lmeta->rts = smeta->rts;
                        lmeta->owner = smeta->owner;

                        // copy data back
                        scc_manager->do_read(&smeta->scc_meta, coordinator_id, local_data, migrated_row_value, table->value_size());

                        // set the migrated row as invalid
                        smeta->is_valid = false;

                        // remove from CXL index
                        CXLTableBase *target_cxl_table = cxl_tbl_vecs[table->tableID()][table->partitionID()];
                        ret = target_cxl_table->remove(key, lmeta->migrated_row);
                        CHECK(ret == true);

                        // mark the local row as not migrated
                        lmeta->migrated_row = nullptr;
                        lmeta->is_migrated = false;

                        // free the CXL row
                        // TODO: register EBR
                        std::size_t row_total_size = sizeof(SundialPashaMetadataShared) + table->value_size();
                        cxl_memory.cxlalloc_free_wrapper(smeta, row_total_size,
                                CXLMemory::DATA_FREE, sizeof(SundialPashaMetadataShared), table->value_size());

                        // release the CXL latch
                        smeta->unlock();
                } else {
                        CHECK(0);
                }
                lmeta->unlock();

                return true;
	}

        bool move_from_btree_to_partition(ITable *table, const void *key, const std::tuple<MetaDataType *, void *> &row)
	{
                MetaDataType &meta = *std::get<0>(row);
		SundialPashaMetadataLocal *lmeta = reinterpret_cast<SundialPashaMetadataLocal *>(meta.load());
                void *local_data = std::get<1>(row);
                bool move_out_success = false;
                bool ret = false;

                auto move_out_processor = [&](const void *prev_key, void *prev_meta, void *prev_data, const void *cur_key, void *cur_meta, void *cur_data, const void *next_key, void *next_meta, void *next_data) {
                        auto prev_lmeta = reinterpret_cast<SundialPashaMetadataLocal *>(prev_meta);
                        auto cur_lmeta = reinterpret_cast<SundialPashaMetadataLocal *>(cur_meta);
                        auto next_lmeta = reinterpret_cast<SundialPashaMetadataLocal *>(next_meta);

                        bool is_cur_tuple_moved_out = false;

                        CHECK(lmeta != nullptr && cur_lmeta == lmeta);

                        // move the current tuple out
                        // note that here we only mark it as invalid without removing it from it CXL index
                        lmeta->lock();
                        CHECK(lmeta->is_valid == true);
                        if (lmeta->is_migrated == true) {
                                SundialPashaMetadataShared *smeta = reinterpret_cast<SundialPashaMetadataShared *>(lmeta->migrated_row);
                                char *migrated_row_value = lmeta->migrated_row + sizeof(SundialPashaMetadataShared);

                                // take the CXL latch
                                smeta->lock();

                                // the tuple must be valid
                                CHECK(smeta->is_valid == true);

                                // reference count > 0, cannot move out the tuple -> early return
                                if (smeta->ref_cnt > 0) {
                                        smeta->unlock();
                                        lmeta->unlock();
                                        move_out_success = false;
                                        return;
                                }

                                // copy metadata back
                                lmeta->wts = smeta->wts;
                                lmeta->rts = smeta->rts;
                                lmeta->owner = smeta->owner;

                                // copy data back
                                scc_manager->do_read(&smeta->scc_meta, coordinator_id, local_data, migrated_row_value, table->value_size());

                                // set the migrated row as invalid
                                smeta->is_valid = false;

                                // remove the current-key from the CXL index
                                // it is safe to do so because there is no concurrent data move in/out
                                CXLTableBase *target_cxl_table = cxl_tbl_vecs[table->tableID()][table->partitionID()];
                                ret = target_cxl_table->remove(key, nullptr);
                                CHECK(ret == true);

                                // mark the local row as not migrated
                                lmeta->migrated_row = nullptr;
                                lmeta->is_migrated = false;

                                // free the CXL row
                                // TODO: register EBR
                                std::size_t row_total_size = sizeof(SundialPashaMetadataShared) + table->value_size();
                                cxl_memory.cxlalloc_free_wrapper(smeta, row_total_size,
                                        CXLMemory::DATA_FREE, sizeof(SundialPashaMetadataShared), table->value_size());

                                // release the CXL latch
                                smeta->unlock();
                        } else {
                                CHECK(0);
                        }
                        lmeta->unlock();

                        // update the next-key information for the previous tuple
                        if (prev_lmeta != nullptr) {
                                prev_lmeta->lock();
                                if (prev_lmeta->is_migrated == true) {
                                        auto prev_smeta = reinterpret_cast<SundialPashaMetadataShared *>(prev_lmeta->migrated_row);
                                        prev_smeta->lock();
                                        prev_smeta->is_next_key_real = false;
                                        prev_smeta->unlock();
                                }
                                prev_lmeta->unlock();
                        }

                        move_out_success = true;
		};

                // update next-key information
                ret = table->search_and_update_next_key_info(key, move_out_processor);
                CHECK(ret == true);

		return move_out_success;
	}

        bool move_from_shared_region_to_partition(ITable *table, const void *key, const std::tuple<MetaDataType *, void *> &row)
	{
                bool move_out_success = false;

                if (table->tableType() == ITable::HASHMAP) {
                        move_out_success = move_from_hashmap_to_partition(table, key, row);
                } else if (table->tableType() == ITable::BTREE) {
                        move_out_success = move_from_btree_to_partition(table, key, row);
                } else {
                        CHECK(0);
                }

                // statistics
                if (move_out_success == true) {
                        // LOG(INFO) << "moved out a row with key " << table->get_plain_key(key) << " from table " << table->tableID();
                        num_data_move_out.fetch_add(1);
                }

		return move_out_success;
	}

        bool delete_and_update_next_key_info(ITable *table, const void *key, bool is_local_delete, bool &need_move_out_from_migration_tracker, void *&migration_policy_meta)
	{
                CHECK(0);
	}

    public:
	static constexpr int LOCK_BIT_OFFSET = 63;
	static constexpr uint64_t LOCK_BIT_MASK = 0x1ull;

    private:
        std::size_t coordinator_id;

        std::vector<std::vector<CXLTableBase *> > &cxl_tbl_vecs;

        std::atomic<uint64_t> init_finished;
};

extern SundialPashaHelper *sundial_pasha_global_helper;

} // namespace star

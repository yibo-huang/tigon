//
// Created by Yi Lu on 9/11/18.
//

#pragma once

#include <atomic>
#include <list>
#include <tuple>
#include <memory>

#include "common/CCSet.h"
#include "common/CCHashTable.h"
#include "core/Table.h"
#include "glog/logging.h"

#include "protocol/Pasha/MigrationManager.h"
#include "protocol/Pasha/SCCManager.h"

#include <boost/interprocess/offset_ptr.hpp>

namespace star
{

struct TwoPLPashaMetadataLocal {
        TwoPLPashaMetadataLocal()
                : tid(0)
                , is_migrated(false)
                , migrated_row(nullptr)
        {
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

	uint64_t tid{ 0 };

        bool is_migrated{ false };
        char *migrated_row{ nullptr };
};

struct TwoPLPashaMetadataShared {
        TwoPLPashaMetadataShared()
                : tid(0)
                , ref_cnt(0)
                , is_valid(false)
                , scc_meta(0)
        {
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

	uint64_t tid{ 0 };

        // multi-host transaction accessing a cxl row would increase its reference count by 1
        // a migrated row can only be moved out if its ref_cnt == 0
        uint64_t ref_cnt{ 0 };

        // a migrated tuple can be invalid if it is deleted or migrated out
        bool is_valid{ false };

        // software cache-coherence metadata
        uint64_t scc_meta{ 0 };         // directly embed it here to avoid extra cxlalloc_malloc
};

uint64_t TwoPLPashaMetadataLocalInit();

class TwoPLPashaHelper {
    public:
	using MetaDataType = std::atomic<uint64_t>;

        TwoPLPashaHelper()
                : coordinator_id(0)
                , coordinator_num(0)
                , table_num_per_partition(0)
                , partition_num_per_host(0)
        {
        }

        TwoPLPashaHelper(std::size_t coordinator_id, std::size_t coordinator_num, 
                std::size_t table_num_per_partition, std::size_t partition_num_per_host)
                : coordinator_id(coordinator_id)
                , coordinator_num(coordinator_num)
                , table_num_per_partition(table_num_per_partition)
                , partition_num_per_host(partition_num_per_host)
        {
        }

	uint64_t read(const std::tuple<MetaDataType *, void *> &row, void *dest, std::size_t size)
	{
                MetaDataType &meta = *std::get<0>(row);
                TwoPLPashaMetadataLocal *lmeta = reinterpret_cast<TwoPLPashaMetadataLocal *>(meta.load());
                uint64_t tid_ = 0;

                lmeta->lock();
                if (lmeta->is_migrated == false) {
		        void *src = std::get<1>(row);
		        std::memcpy(dest, src, size);
                        tid_ = lmeta->tid;
                } else {
                        TwoPLPashaMetadataShared *smeta = reinterpret_cast<TwoPLPashaMetadataShared *>(lmeta->migrated_row);
                        void *src = lmeta->migrated_row + sizeof(TwoPLPashaMetadataShared);
                        smeta->lock();
                        CHECK(smeta->is_valid == true);
                        scc_manager->do_read(&smeta->scc_meta, coordinator_id, dest, src, size);
                        tid_ = smeta->tid;
                        smeta->unlock();
                }
                lmeta->unlock();

		return remove_lock_bit(tid_);
	}

        uint64_t remote_read(char *row, void *dest, std::size_t size)
	{
                DCHECK(row != nullptr);
		TwoPLPashaMetadataShared *smeta = reinterpret_cast<TwoPLPashaMetadataShared *>(row);
                void *src = row + sizeof(TwoPLPashaMetadataShared);
                uint64_t tid_ = 0;

		smeta->lock();
                CHECK(smeta->is_valid == true);
                scc_manager->do_read(&smeta->scc_meta, coordinator_id, dest, src, size);
                tid_ = smeta->tid;
		smeta->unlock();

		return remove_lock_bit(tid_);
	}

        void update(const std::tuple<MetaDataType *, void *> &row, const void *value, std::size_t value_size)
	{
		MetaDataType &meta = *std::get<0>(row);
                TwoPLPashaMetadataLocal *lmeta = reinterpret_cast<TwoPLPashaMetadataLocal *>(meta.load());

		lmeta->lock();
                if (lmeta->is_migrated == false) {
                        void *data_ptr = std::get<1>(row);
                        memcpy(data_ptr, value, value_size);
                } else {
                        TwoPLPashaMetadataShared *smeta = reinterpret_cast<TwoPLPashaMetadataShared *>(lmeta->migrated_row);
                        void *data_ptr = lmeta->migrated_row + sizeof(TwoPLPashaMetadataShared);
                        smeta->lock();
                        CHECK(smeta->is_valid == true);
                        scc_manager->do_write(&smeta->scc_meta, coordinator_id, data_ptr, value, value_size);
                        smeta->unlock();
                }
		lmeta->unlock();
	}

        void remote_update(char *row, const void *value, std::size_t value_size)
	{
                DCHECK(row != nullptr);
		TwoPLPashaMetadataShared *smeta = reinterpret_cast<TwoPLPashaMetadataShared *>(row);
                void *data_ptr = row + sizeof(TwoPLPashaMetadataShared);

		smeta->lock();
                CHECK(smeta->is_valid == true);
                scc_manager->do_write(&smeta->scc_meta, coordinator_id, data_ptr, value, value_size);
                smeta->unlock();
	}

	/**
	 * [write lock bit (1) |  read lock bit (9) -- 512 - 1 locks | seq id  (54) ]
	 *
	 */

	static bool is_read_locked(uint64_t value)
	{
		return value & (READ_LOCK_BIT_MASK << READ_LOCK_BIT_OFFSET);
	}

	static bool is_write_locked(uint64_t value)
	{
		return value & (WRITE_LOCK_BIT_MASK << WRITE_LOCK_BIT_OFFSET);
	}

	static uint64_t read_lock_num(uint64_t value)
	{
		return (value >> READ_LOCK_BIT_OFFSET) & READ_LOCK_BIT_MASK;
	}

	static uint64_t read_lock_max()
	{
		return READ_LOCK_BIT_MASK;
	}

	static uint64_t read_lock(std::atomic<uint64_t> &meta, bool &success)
	{
                TwoPLPashaMetadataLocal *lmeta = reinterpret_cast<TwoPLPashaMetadataLocal *>(meta.load());
                uint64_t old_value, new_value;

                lmeta->lock();
                if (lmeta->is_migrated == false) {
                        old_value = lmeta->tid;

                        // can we get the lock?
                        if (is_write_locked(old_value) || read_lock_num(old_value) == read_lock_max()) {
                                success = false;
                                goto out_unlock_lmeta;
                        }

                        // OK, we can get the lock
                        new_value = old_value + (1ull << READ_LOCK_BIT_OFFSET);
                        lmeta->tid = new_value;
                        success = true;
                } else {
                        TwoPLPashaMetadataShared *smeta = reinterpret_cast<TwoPLPashaMetadataShared *>(lmeta->migrated_row);
                        smeta->lock();
                        CHECK(smeta->is_valid == true);

                        old_value = smeta->tid;

                        // can we get the lock?
                        if (is_write_locked(old_value) || read_lock_num(old_value) == read_lock_max()) {
                                success = false;
                                smeta->unlock();
                                goto out_unlock_lmeta;
                        }

                        // OK, we can get the lock
                        new_value = old_value + (1ull << READ_LOCK_BIT_OFFSET);
                        smeta->tid = new_value;
                        success = true;

                        smeta->unlock();
                }

out_unlock_lmeta:
                lmeta->unlock();
		return remove_lock_bit(old_value);
	}

        static uint64_t remote_read_lock(char *row, bool &success)
	{
                DCHECK(row != nullptr);
		TwoPLPashaMetadataShared *smeta = reinterpret_cast<TwoPLPashaMetadataShared *>(row);
                uint64_t old_value, new_value;

		smeta->lock();
                CHECK(smeta->is_valid == true);

                old_value = smeta->tid;

                // can we get the lock?
                if (is_write_locked(old_value) || read_lock_num(old_value) == read_lock_max()) {
                        success = false;
                        smeta->unlock();
                        return remove_lock_bit(old_value);
                }

                // OK, we can get the lock
                new_value = old_value + (1ull << READ_LOCK_BIT_OFFSET);
                smeta->tid = new_value;
                success = true;

                smeta->unlock();

		return remove_lock_bit(old_value);
	}


	static uint64_t write_lock(std::atomic<uint64_t> &meta, bool &success)
	{
                TwoPLPashaMetadataLocal *lmeta = reinterpret_cast<TwoPLPashaMetadataLocal *>(meta.load());
                uint64_t old_value, new_value;

                lmeta->lock();
                if (lmeta->is_migrated == false) {
                        old_value = lmeta->tid;

                        // can we get the lock?
                        if (is_read_locked(old_value) || is_write_locked(old_value)) {
                                success = false;
                                lmeta->unlock();
                                return remove_lock_bit(old_value);
                        }

                        // OK, we can get the lock
                        new_value = old_value + (WRITE_LOCK_BIT_MASK << WRITE_LOCK_BIT_OFFSET);
                        lmeta->tid = new_value;
                        success = true;
                } else {
                        TwoPLPashaMetadataShared *smeta = reinterpret_cast<TwoPLPashaMetadataShared *>(lmeta->migrated_row);
                        smeta->lock();
                        CHECK(smeta->is_valid == true);

                        old_value = smeta->tid;

                        // can we get the lock?
                        if (is_read_locked(old_value) || is_write_locked(old_value)) {
                                success = false;
                                smeta->unlock();
                                goto out_unlock_lmeta;
                        }

                        // OK, we can get the lock
                        new_value = old_value + (WRITE_LOCK_BIT_MASK << WRITE_LOCK_BIT_OFFSET);
                        smeta->tid = new_value;
                        success = true;

                        smeta->unlock();
                }

out_unlock_lmeta:
                lmeta->unlock();
		return remove_lock_bit(old_value);
	}

        static uint64_t remote_write_lock(char *row, bool &success)
	{
                DCHECK(row != nullptr);
		TwoPLPashaMetadataShared *smeta = reinterpret_cast<TwoPLPashaMetadataShared *>(row);
                uint64_t old_value, new_value;

		smeta->lock();
                CHECK(smeta->is_valid == true);

                old_value = smeta->tid;

                // can we get the lock?
                if (is_read_locked(old_value) || is_write_locked(old_value)) {
                        success = false;
                        smeta->unlock();
                        return remove_lock_bit(old_value);
                }

                // OK, we can get the lock
                new_value = old_value + (WRITE_LOCK_BIT_MASK << WRITE_LOCK_BIT_OFFSET);
                smeta->tid = new_value;
                success = true;

                smeta->unlock();

		return remove_lock_bit(old_value);
	}

	static void read_lock_release(std::atomic<uint64_t> &meta)
	{
                TwoPLPashaMetadataLocal *lmeta = reinterpret_cast<TwoPLPashaMetadataLocal *>(meta.load());
                uint64_t old_value, new_value;

                lmeta->lock();
                if (lmeta->is_migrated == false) {
                        old_value = lmeta->tid;
			DCHECK(is_read_locked(old_value));
			DCHECK(!is_write_locked(old_value));
			new_value = old_value - (1ull << READ_LOCK_BIT_OFFSET);
                        lmeta->tid = new_value;
                } else {
                        TwoPLPashaMetadataShared *smeta = reinterpret_cast<TwoPLPashaMetadataShared *>(lmeta->migrated_row);
                        smeta->lock();
                        CHECK(smeta->is_valid == true);

                        old_value = smeta->tid;
			DCHECK(is_read_locked(old_value));
			DCHECK(!is_write_locked(old_value));
			new_value = old_value - (1ull << READ_LOCK_BIT_OFFSET);
                        smeta->tid = new_value;

                        smeta->unlock();
                }
                lmeta->unlock();
	}

        static void remote_read_lock_release(char *row)
	{
                DCHECK(row != nullptr);
		TwoPLPashaMetadataShared *smeta = reinterpret_cast<TwoPLPashaMetadataShared *>(row);
                uint64_t old_value, new_value;

		smeta->lock();
                CHECK(smeta->is_valid == true);

                old_value = smeta->tid;
                DCHECK(is_read_locked(old_value));
                DCHECK(!is_write_locked(old_value));
                new_value = old_value - (1ull << READ_LOCK_BIT_OFFSET);
                smeta->tid = new_value;

                smeta->unlock();
	}

	static void write_lock_release(std::atomic<uint64_t> &meta)
	{
                TwoPLPashaMetadataLocal *lmeta = reinterpret_cast<TwoPLPashaMetadataLocal *>(meta.load());
                uint64_t old_value, new_value;

                lmeta->lock();
                if (lmeta->is_migrated == false) {
                        old_value = lmeta->tid;
                        DCHECK(!is_read_locked(old_value));
                        DCHECK(is_write_locked(old_value));
                        new_value = old_value - (1ull << WRITE_LOCK_BIT_OFFSET);
                        lmeta->tid = new_value;
                } else {
                        TwoPLPashaMetadataShared *smeta = reinterpret_cast<TwoPLPashaMetadataShared *>(lmeta->migrated_row);
                        smeta->lock();
                        CHECK(smeta->is_valid == true);

                        old_value = smeta->tid;
                        DCHECK(!is_read_locked(old_value));
                        DCHECK(is_write_locked(old_value));
                        new_value = old_value - (1ull << WRITE_LOCK_BIT_OFFSET);
                        smeta->tid = new_value;

                        smeta->unlock();
                }
                lmeta->unlock();
	}

        static void remote_write_lock_release(char *row)
	{
                DCHECK(row != nullptr);
		TwoPLPashaMetadataShared *smeta = reinterpret_cast<TwoPLPashaMetadataShared *>(row);
                uint64_t old_value, new_value;

		smeta->lock();
                CHECK(smeta->is_valid == true);

                old_value = smeta->tid;
                DCHECK(!is_read_locked(old_value));
                DCHECK(is_write_locked(old_value));
                new_value = old_value - (1ull << WRITE_LOCK_BIT_OFFSET);
                smeta->tid = new_value;

                smeta->unlock();
	}

	static void write_lock_release(std::atomic<uint64_t> &meta, uint64_t new_value)
	{
                TwoPLPashaMetadataLocal *lmeta = reinterpret_cast<TwoPLPashaMetadataLocal *>(meta.load());
                uint64_t old_value;

                lmeta->lock();
                if (lmeta->is_migrated == false) {
                        old_value = lmeta->tid;
                        DCHECK(!is_read_locked(old_value));
                        DCHECK(is_write_locked(old_value));
                        DCHECK(!is_read_locked(new_value));
                        DCHECK(!is_write_locked(new_value));
                        lmeta->tid = new_value;
                } else {
                        TwoPLPashaMetadataShared *smeta = reinterpret_cast<TwoPLPashaMetadataShared *>(lmeta->migrated_row);
                        smeta->lock();
                        CHECK(smeta->is_valid == true);

                        old_value = smeta->tid;
                        DCHECK(!is_read_locked(old_value));
                        DCHECK(is_write_locked(old_value));
                        DCHECK(!is_read_locked(new_value));
                        DCHECK(!is_write_locked(new_value));
                        smeta->tid = new_value;

                        smeta->unlock();
                }
                lmeta->unlock();
	}

        static void remote_write_lock_release(char *row, uint64_t new_value)
	{
                DCHECK(row != nullptr);
		TwoPLPashaMetadataShared *smeta = reinterpret_cast<TwoPLPashaMetadataShared *>(row);
                uint64_t old_value;

		smeta->lock();
                CHECK(smeta->is_valid == true);

                old_value = smeta->tid;
                DCHECK(!is_read_locked(old_value));
                DCHECK(is_write_locked(old_value));
                DCHECK(!is_read_locked(new_value));
                DCHECK(!is_write_locked(new_value));
                smeta->tid = new_value;

                smeta->unlock();
	}

	static uint64_t remove_lock_bit(uint64_t value)
	{
		return value & ~(LOCK_BIT_MASK << LOCK_BIT_OFFSET);
	}

	static uint64_t remove_read_lock_bit(uint64_t value)
	{
		return value & ~(READ_LOCK_BIT_MASK << READ_LOCK_BIT_OFFSET);
	}

	static uint64_t remove_write_lock_bit(uint64_t value)
	{
		return value & ~(WRITE_LOCK_BIT_MASK << WRITE_LOCK_BIT_OFFSET);
	}

        void init_pasha_metadata()
        {
                uint64_t total_partition_num = partition_num_per_host * coordinator_num;
                uint64_t total_table_num = table_num_per_partition * total_partition_num;

                cxl_tbl_vecs.resize(table_num_per_partition);

                // init CXL hash tables
                if (coordinator_id == 0) {
                        cxl_hashtables = reinterpret_cast<CCHashTable *>(cxl_memory.cxlalloc_malloc_wrapper(sizeof(CCHashTable) * total_table_num,
                                CXLMemory::INDEX_ALLOCATION));
                        for (int i = 0; i < table_num_per_partition; i++) {
                                cxl_tbl_vecs[i].resize(total_partition_num);
                                for (int j = 0; j < total_partition_num; j++) {
                                        CCHashTable *cxl_table = &cxl_hashtables[i * total_partition_num + j];
                                        new(cxl_table) CCHashTable(cxl_hashtable_bkt_cnt);
                                        cxl_tbl_vecs[i][j] = cxl_table;
                                }
                        }
                        CXLMemory::commit_shared_data_initialization(CXLMemory::cxl_data_migration_root_index, cxl_hashtables);
                        LOG(INFO) << "TwoPLPasha Helper initializes data migration metadata ("
                                << total_table_num << " CXL hash tables each with " << cxl_hashtable_bkt_cnt << " entries)";
                } else {
                        void *tmp = NULL;
                        CXLMemory::wait_and_retrieve_cxl_shared_data(CXLMemory::cxl_data_migration_root_index, &tmp);
                        cxl_hashtables = reinterpret_cast<CCHashTable *>(tmp);
                        for (int i = 0; i < table_num_per_partition; i++) {
                                cxl_tbl_vecs[i].resize(total_partition_num);
                                for (int j = 0; j < total_partition_num; j++) {
                                        CCHashTable *cxl_table = &cxl_hashtables[i * total_partition_num + j];
                                        cxl_tbl_vecs[i][j] = cxl_table;
                                }
                        }
                        LOG(INFO) << "TwoPLPasha Helper retrieves data migration metadata ("
                                << total_table_num << " CXL hash tables each with " << cxl_hashtable_bkt_cnt << " entries)";
                }
        }

        void commit_pasha_metadata_init()
        {
                init_finished.store(1, std::memory_order_release);
        }

        void wait_for_pasha_metadata_init()
        {
                while (init_finished.load(std::memory_order_acquire) == 0);
        }

        char *get_migrated_row(std::size_t table_id, std::size_t partition_id, uint64_t plain_key, bool inc_ref_cnt)
        {
                CCHashTable *target_cxl_table = cxl_tbl_vecs[table_id][partition_id];
                char *migrated_row = target_cxl_table->search(plain_key);

                if (migrated_row != nullptr) {
                        TwoPLPashaMetadataShared *smeta = reinterpret_cast<TwoPLPashaMetadataShared *>(migrated_row);
                        if (inc_ref_cnt == true) {
                                smeta->lock();
                                if (smeta->is_valid == true) {
                                        smeta->ref_cnt++;
                                } else {
                                        migrated_row = nullptr;
                                }
                                smeta->unlock();
                        }
                }
                return migrated_row;
        }

        void release_migrated_row(std::size_t table_id, std::size_t partition_id, uint64_t plain_key)
        {
                CCHashTable *target_cxl_table = cxl_tbl_vecs[table_id][partition_id];
                char *migrated_row = target_cxl_table->search(plain_key);
                CHECK(migrated_row != nullptr);

                TwoPLPashaMetadataShared *smeta = reinterpret_cast<TwoPLPashaMetadataShared *>(migrated_row);
                smeta->lock();
                CHECK(smeta->is_valid == true);
                CHECK(smeta->ref_cnt > 0);
                smeta->ref_cnt--;
                smeta->unlock();
        }

        bool move_from_partition_to_shared_region(ITable *table, uint64_t plain_key, const std::tuple<MetaDataType *, void *> &row)
	{
                MetaDataType &meta = *std::get<0>(row);
                TwoPLPashaMetadataLocal *lmeta = reinterpret_cast<TwoPLPashaMetadataLocal *>(meta.load());
                bool ret = false;

		lmeta->lock();
                if (lmeta->is_migrated == false) {
                        void *local_data = std::get<1>(row);

                        // allocate the CXL row
                        std::size_t row_total_size = sizeof(TwoPLPashaMetadataShared) + table->value_size();
                        char *migrated_row_ptr = reinterpret_cast<char *>(cxl_memory.cxlalloc_malloc_wrapper(row_total_size, CXLMemory::DATA_ALLOCATION));
                        char *migrated_row_value_ptr = migrated_row_ptr + sizeof(TwoPLPashaMetadataShared);
                        TwoPLPashaMetadataShared *smeta = reinterpret_cast<TwoPLPashaMetadataShared *>(migrated_row_ptr);
                        new(smeta) TwoPLPashaMetadataShared();

                        // init software cache-coherence metadata
                        scc_manager->init_scc_metadata(&smeta->scc_meta, coordinator_id);

                        // take the CXL latch
                        smeta->lock();

                        // copy metadata
                        smeta->tid = lmeta->tid;

                        // copy data
                        std::memcpy(migrated_row_value_ptr, local_data, table->value_size());

                        // set the migrated row as valid
                        smeta->is_valid = true;

                        // increase the reference count for the requesting host
                        smeta->ref_cnt++;

                        // insert into CXL hash tables
                        CCHashTable *target_cxl_table = cxl_tbl_vecs[table->tableID()][table->partitionID()];
                        ret = target_cxl_table->insert(plain_key, migrated_row_ptr);
                        DCHECK(ret == true);

                        // mark the local row as migrated
                        lmeta->migrated_row = migrated_row_ptr;
                        lmeta->is_migrated = true;

                        // release the CXL latch
                        smeta->unlock();

                        // LOG(INFO) << "moved in a row with key " << plain_key << " from table " << table->tableID();

                        ret = true;

                        // statistic
                        num_data_move_in.fetch_add(1);
                } else {
                        // increase the reference count for the requesting host, even if it is already migrated
                        TwoPLPashaMetadataShared *smeta = reinterpret_cast<TwoPLPashaMetadataShared *>(lmeta->migrated_row);

                        smeta->lock();
                        CHECK(smeta->is_valid == true);
                        smeta->ref_cnt++;
                        smeta->unlock();
                        ret = false;
                }
		lmeta->unlock();

		return ret;
	}

        // TODO: currently migrating data out is not supported
        // need to modify other functions to ensure consistency
        bool move_from_shared_region_to_partition(ITable *table, uint64_t plain_key, const std::tuple<MetaDataType *, void *> &row)
	{
                MetaDataType &meta = *std::get<0>(row);
                TwoPLPashaMetadataLocal *lmeta = reinterpret_cast<TwoPLPashaMetadataLocal *>(meta.load());
                bool ret = false;

                lmeta->lock();
                if (lmeta->is_migrated == true) {
                        void *local_data = std::get<1>(row);
                        TwoPLPashaMetadataShared *smeta = reinterpret_cast<TwoPLPashaMetadataShared *>(lmeta->migrated_row);
                        char *migrated_row_value = lmeta->migrated_row + sizeof(TwoPLPashaMetadataShared);

                        // take the CXL latch
                        smeta->lock();
                        DCHECK(smeta->is_valid == true);

                        // reference count > 0, cannot move out the tuple
                        if (smeta->ref_cnt > 0) {
                                smeta->unlock();
                                lmeta->unlock();
                                return false;
                        }

                        // copy metadata back
                        lmeta->tid = smeta->tid;

                        // copy data back
                        scc_manager->do_read(&smeta->scc_meta, coordinator_id, local_data, migrated_row_value, table->value_size());

                        // set the migrated row as invalid
                        smeta->is_valid = false;

                        // remove from CXL index
                        CCHashTable *target_cxl_table = cxl_tbl_vecs[table->tableID()][table->partitionID()];
                        ret = target_cxl_table->remove(plain_key, lmeta->migrated_row);
                        DCHECK(ret == true);

                        // mark the local row as not migrated
                        lmeta->migrated_row = nullptr;
                        lmeta->is_migrated = false;

                        // free the CXL row
                        // TODO: register EBR
                        std::size_t row_total_size = sizeof(TwoPLPashaMetadataShared) + table->value_size();
                        cxl_memory.cxlalloc_free_wrapper(smeta, row_total_size, CXLMemory::DATA_FREE);

                        // release the CXL latch
                        smeta->unlock();

                        // LOG(INFO) << "moved out a row with key " << plain_key << " from table " << table->tableID();

                        ret = true;

                        // statistic
                        num_data_move_out.fetch_add(1);
                } else {
                        CHECK(0);
                }
                lmeta->unlock();

                return ret;
	}

    public:
	static constexpr int LOCK_BIT_OFFSET = 54;
	static constexpr uint64_t LOCK_BIT_MASK = 0x3ffull;

	static constexpr int READ_LOCK_BIT_OFFSET = 54;
	static constexpr uint64_t READ_LOCK_BIT_MASK = 0x1ffull;

	static constexpr int WRITE_LOCK_BIT_OFFSET = 63;
	static constexpr uint64_t WRITE_LOCK_BIT_MASK = 0x1ull;

    private:
        static constexpr uint64_t cxl_hashtable_bkt_cnt = 50000;

        std::size_t coordinator_id;
        std::size_t coordinator_num;
        std::size_t table_num_per_partition;
        std::size_t partition_num_per_host;

        CCHashTable *cxl_hashtables;
        std::vector<std::vector<CCHashTable *> > cxl_tbl_vecs;

        std::atomic<uint64_t> init_finished;
};

extern TwoPLPashaHelper twopl_pasha_global_helper;

} // namespace star

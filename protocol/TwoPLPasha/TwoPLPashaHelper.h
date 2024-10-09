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
        pthread_spinlock_t latch;
        void lock()
	{
                pthread_spin_lock(&latch);
	}

	void unlock()
	{
		pthread_spin_unlock(&latch);
	}

	uint64_t tid{ 0 };

        bool is_migrated{ false };
        char *migrated_row{ nullptr };
};

struct TwoPLPashaMetadataShared {
        pthread_spinlock_t latch;
        void lock()
	{
                pthread_spin_lock(&latch);
	}

	void unlock()
	{
		pthread_spin_unlock(&latch);
	}

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

	static uint64_t read(const std::tuple<MetaDataType *, void *> &row, void *dest, std::size_t size)
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
                        CHECK(0);
                }
                lmeta->unlock();

		return remove_lock_bit(tid_);
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
                                lmeta->unlock();
                                return remove_lock_bit(old_value);
                        }

                        // OK, we can get the lock
                        new_value = old_value + (1ull << READ_LOCK_BIT_OFFSET);
                        lmeta->tid = new_value;
                        success = true;
                } else {
                        CHECK(0);
                }
                lmeta->unlock();

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
                        CHECK(0);
                }
                lmeta->unlock();

		return remove_lock_bit(old_value);
	}

	static uint64_t write_lock(std::atomic<uint64_t> &meta)
	{
                uint64_t old_value = 0;
                bool success = false;

                while (true) {
                        old_value = write_lock(meta, success);
                        if (success == true) {
                                break;
                        }
                }

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
                        CHECK(0);
                }
                lmeta->unlock();
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
                        CHECK(0);
                }
                lmeta->unlock();
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
                        CHECK(0);
                }
                lmeta->unlock();
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

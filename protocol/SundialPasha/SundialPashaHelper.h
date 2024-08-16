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
#include "core/Table.h"
#include "glog/logging.h"

namespace star
{

struct SundialPashaMetadataLocal {
	std::atomic<uint64_t> latch{ 0 };
	void lock()
	{
retry:
		auto v = latch.load();
		if (v == 0) {
			if (latch.compare_exchange_strong(v, 1))
				return;
			goto retry;
		} else {
			goto retry;
		}
	}

	void unlock()
	{
		DCHECK(latch.load() == 1);
		latch.store(0);
	}

	uint64_t wts{ 0 };
	uint64_t rts{ 0 };
	uint64_t owner{ 0 };

        bool is_migrated{ false };
        char *migrated_row{ nullptr };
};

struct SundialPashaMetadataShared {
	std::atomic<uint64_t> latch{ 0 };
	void lock()
	{
retry:
		auto v = latch.load();
		if (v == 0) {
			if (latch.compare_exchange_strong(v, 1))
				return;
			goto retry;
		} else {
			goto retry;
		}
	}

	void unlock()
	{
		DCHECK(latch.load() == 1);
		latch.store(0);
	}

	uint64_t wts{ 0 };
	uint64_t rts{ 0 };
	uint64_t owner{ 0 };
};

uint64_t SundialPashaMetadataLocalInit();
uint64_t SundialPashaMetadataSharedInit();

/* 
 * lmeta means local metadata stored in local DRAM
 * smeta means shared metadata stored in the shared region
 */
class SundialPashaHelper {
    public:
        using MetaDataType = std::atomic<uint64_t>;

        SundialPashaHelper()
                : coordinator_id(0)
                , coordinator_num(0)
                , table_num_per_partition(0)
                , partition_num_per_host(0)
        {
        }

        SundialPashaHelper(std::size_t coordinator_id, std::size_t coordinator_num, 
                std::size_t table_num_per_partition, std::size_t partition_num_per_host)
                : coordinator_id(coordinator_id)
                , coordinator_num(coordinator_num)
                , table_num_per_partition(table_num_per_partition)
                , partition_num_per_host(partition_num_per_host)
        {
        }

	static uint64_t get_or_install_meta(std::atomic<uint64_t> &ptr)
	{
retry:
		auto v = ptr.load();
		if (v != 0) {
			return v;
		}
		auto meta_ptr = SundialPashaMetadataLocalInit();
		if (ptr.compare_exchange_strong(v, meta_ptr) == false) {
			delete ((SundialPashaMetadataLocal *)meta_ptr);
			goto retry;
		}
		return meta_ptr;
	}

	// Returns <rts, wts> of the tuple.
	static std::pair<uint64_t, uint64_t> read(const std::tuple<MetaDataType *, void *> &row, void *dest, std::size_t size)
	{
                uint64_t rts = 0, wts = 0;
		MetaDataType &meta = *std::get<0>(row);
		SundialPashaMetadataLocal *lmeta = reinterpret_cast<SundialPashaMetadataLocal *>(get_or_install_meta(meta));
		DCHECK(lmeta != nullptr);

		lmeta->lock();
                if (lmeta->is_migrated == false) {
                        void *src = std::get<1>(row);
                        rts = lmeta->rts;
                        wts = lmeta->wts;
                        std::memcpy(dest, src, size);
                } else {
                        SundialPashaMetadataShared *smeta = reinterpret_cast<SundialPashaMetadataShared *>(lmeta->migrated_row);
                        void *src = lmeta->migrated_row + sizeof(SundialPashaMetadataShared);
                        smeta->lock();
                        rts = smeta->rts;
                        wts = smeta->wts;
                        std::memcpy(dest, src, size);
                        smeta->unlock();
                }
		lmeta->unlock();

		return std::make_pair(wts, rts);
	}

	// Returns <rts, wts> of the tuple.
	static bool write_lock(const std::tuple<MetaDataType *, void *> &row, std::pair<uint64_t, uint64_t> &rwts, uint64_t transaction_id)
	{
		MetaDataType &meta = *std::get<0>(row);
		SundialPashaMetadataLocal *lmeta = reinterpret_cast<SundialPashaMetadataLocal *>(get_or_install_meta(meta));
		bool success = false;

		lmeta->lock();
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
                        rwts.first = smeta->wts;
                        rwts.second = smeta->rts;
                        if (smeta->owner == 0 || smeta->owner == transaction_id) {
                                success = true;
                                smeta->owner = transaction_id;
                        }
                        smeta->unlock();
                }
		lmeta->unlock();

		return success;
	}

	static bool renew_lease(const std::tuple<MetaDataType *, void *> &row, uint64_t wts, uint64_t commit_ts)
	{
		MetaDataType &meta = *std::get<0>(row);
		SundialPashaMetadataLocal *lmeta = reinterpret_cast<SundialPashaMetadataLocal *>(get_or_install_meta(meta));
		bool success = false;

		lmeta->lock();
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
                        if (wts != smeta->wts || (commit_ts > smeta->rts && smeta->owner != 0)) {
                                success = false;
                        } else {
                                success = true;
                                smeta->rts = std::max(smeta->rts, commit_ts);
                        }
                        smeta->unlock();
                }
		lmeta->unlock();

		return success;
	}

	static void replica_update(const std::tuple<MetaDataType *, void *> &row, const void *value, std::size_t value_size, uint64_t commit_ts)
	{
		MetaDataType &meta = *std::get<0>(row);
		SundialPashaMetadataLocal *lmeta = reinterpret_cast<SundialPashaMetadataLocal *>(get_or_install_meta(meta));

		lmeta->lock();
                if (lmeta->is_migrated == false) {
                        void *data_ptr = std::get<1>(row);
                        DCHECK(lmeta->wts == lmeta->rts);
                        if (commit_ts > lmeta->wts) { // Thomas write rule
                                lmeta->wts = lmeta->rts = commit_ts;
                                memcpy(data_ptr, value, value_size);
                        }
                } else {
                        SundialPashaMetadataShared *smeta = reinterpret_cast<SundialPashaMetadataShared *>(lmeta->migrated_row);
                        void *data_ptr = lmeta->migrated_row + sizeof(SundialPashaMetadataShared);
                        smeta->lock();
                        DCHECK(smeta->wts == smeta->rts);
                        if (commit_ts > smeta->wts) { // Thomas write rule
                                smeta->wts = smeta->rts = commit_ts;
                                memcpy(data_ptr, value, value_size);
                        }
                        smeta->unlock();
                }
		lmeta->unlock();
	}

	static void update(const std::tuple<MetaDataType *, void *> &row, const void *value, std::size_t value_size, uint64_t commit_ts,
			   uint64_t transaction_id)
	{
		MetaDataType &meta = *std::get<0>(row);
		SundialPashaMetadataLocal *lmeta = reinterpret_cast<SundialPashaMetadataLocal *>(get_or_install_meta(meta));

		lmeta->lock();
                if (lmeta->is_migrated == false) {
                        void *data_ptr = std::get<1>(row);
                        CHECK(lmeta->owner == transaction_id);
                        memcpy(data_ptr, value, value_size);
                        lmeta->wts = lmeta->rts = commit_ts;
                } else {
                        SundialPashaMetadataShared *smeta = reinterpret_cast<SundialPashaMetadataShared *>(lmeta->migrated_row);
                        void *data_ptr = lmeta->migrated_row + sizeof(SundialPashaMetadataShared);
                        smeta->lock();
                        CHECK(smeta->owner == transaction_id);
                        memcpy(data_ptr, value, value_size);
                        smeta->wts = smeta->rts = commit_ts;
                        smeta->unlock();
                }
		lmeta->unlock();
	}

	static void update_unlock(const std::tuple<MetaDataType *, void *> &row, const void *value, std::size_t value_size, uint64_t commit_ts,
				  uint64_t transaction_id)
	{
		MetaDataType &meta = *std::get<0>(row);
		SundialPashaMetadataLocal *lmeta = reinterpret_cast<SundialPashaMetadataLocal *>(get_or_install_meta(meta));

		lmeta->lock();
                if (lmeta->is_migrated == false) {
		        void *data_ptr = std::get<1>(row);
                        CHECK(lmeta->owner == transaction_id);
                        memcpy(data_ptr, value, value_size);
                        lmeta->wts = lmeta->rts = commit_ts;
                        lmeta->owner = 0;
                } else {
                        SundialPashaMetadataShared *smeta = reinterpret_cast<SundialPashaMetadataShared *>(lmeta->migrated_row);
                        void *data_ptr = lmeta->migrated_row + sizeof(SundialPashaMetadataShared);
                        smeta->lock();
                        CHECK(smeta->owner == transaction_id);
                        memcpy(data_ptr, value, value_size);
                        smeta->wts = smeta->rts = commit_ts;
                        smeta->owner = 0;
                        smeta->unlock();
                }
		lmeta->unlock();
	}

	static void unlock(const std::tuple<MetaDataType *, void *> &row, uint64_t transaction_id)
	{
		MetaDataType &meta = *std::get<0>(row);
		SundialPashaMetadataLocal *lmeta = reinterpret_cast<SundialPashaMetadataLocal *>(get_or_install_meta(meta));

		lmeta->lock();
                if (lmeta->is_migrated == false) {
                        CHECK(lmeta->owner == transaction_id);
		        lmeta->owner = 0;
                } else {
                        SundialPashaMetadataShared *smeta = reinterpret_cast<SundialPashaMetadataShared *>(lmeta->migrated_row);
                        smeta->lock();
                        CHECK(smeta->owner == transaction_id);
		        smeta->owner = 0;
                        smeta->unlock();
                }
		lmeta->unlock();
	}

	static bool is_locked(uint64_t value)
	{
		return (value >> LOCK_BIT_OFFSET) & LOCK_BIT_MASK;
	}

	static uint64_t lock(std::atomic<uint64_t> &a)
	{
		uint64_t oldValue, newValue;
		do {
			do {
				oldValue = a.load();
			} while (is_locked(oldValue));
			newValue = (LOCK_BIT_MASK << LOCK_BIT_OFFSET) | oldValue;
		} while (!a.compare_exchange_weak(oldValue, newValue));
		DCHECK(is_locked(oldValue) == false);
		return oldValue;
	}

	static uint64_t lock(std::atomic<uint64_t> &a, bool &success)
	{
		uint64_t oldValue = a.load();

		if (is_locked(oldValue)) {
			success = false;
		} else {
			uint64_t newValue = (LOCK_BIT_MASK << LOCK_BIT_OFFSET) | oldValue;
			success = a.compare_exchange_strong(oldValue, newValue);
		}
		return oldValue;
	}

	static void unlock(std::atomic<uint64_t> &a)
	{
		uint64_t oldValue = a.load();
		DCHECK(is_locked(oldValue));
		uint64_t newValue = remove_lock_bit(oldValue);
		bool ok = a.compare_exchange_strong(oldValue, newValue);
		DCHECK(ok);
	}

	static void unlock(std::atomic<uint64_t> &a, uint64_t newValue)
	{
		uint64_t oldValue = a.load();
		DCHECK(is_locked(oldValue));
		DCHECK(is_locked(newValue) == false);
		bool ok = a.compare_exchange_strong(oldValue, newValue);
		DCHECK(ok);
	}

	static uint64_t remove_lock_bit(uint64_t value)
	{
		return value & ~(LOCK_BIT_MASK << LOCK_BIT_OFFSET);
	}

        void init_pasha_metadata()
        {
                uint64_t total_partition_num = partition_num_per_host * coordinator_num;
                uint64_t total_table_num = table_num_per_partition * total_partition_num;

                cxl_tbl_vecs.resize(table_num_per_partition);

                // init CXL hash tables
                if (coordinator_id == 0) {
                        cxl_hashtables = reinterpret_cast<CCHashTable *>(CXLMemory::cxlalloc_malloc_wrapper(sizeof(CCHashTable) * total_table_num));
                        for (int i = 0; i < table_num_per_partition; i++) {
                                cxl_tbl_vecs[i].resize(total_partition_num);
                                for (int j = 0; j < total_partition_num; j++) {
                                        CCHashTable *cxl_table = &cxl_hashtables[i * total_partition_num + j];
                                        new(cxl_table) CCHashTable(cxl_hashtable_bkt_cnt);
                                        cxl_tbl_vecs[i][j] = cxl_table;
                                }
                        }
                        CXLMemory::commit_shared_data_initialization(CXLMemory::cxl_data_migration_root_index, cxl_hashtables);
                        LOG(INFO) << "SundialPasha Helper initializes data migration metadata ("
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
                        LOG(INFO) << "SundialPasha Helper retrieves data migration metadata ("
                                << total_table_num << " CXL hash tables each with " << cxl_hashtable_bkt_cnt << " entries)";
                }

                init_finished.store(1, std::memory_order_release);
        }

        void wait_for_pasha_metadata_init()
        {
                while (init_finished.load(std::memory_order_acquire) == 0);
        }

        bool move_from_partition_to_shared_region(ITable *table, uint64_t plain_key, const std::tuple<MetaDataType *, void *> &row)
	{
                MetaDataType &meta = *std::get<0>(row);
                DCHECK(0 != meta.load());
		SundialPashaMetadataLocal *lmeta = reinterpret_cast<SundialPashaMetadataLocal *>(get_or_install_meta(meta));
		DCHECK(lmeta != nullptr);

                bool ret = false;

		lmeta->lock();
                if (lmeta->is_migrated == false) {
                        void *local_data = std::get<1>(row);

                        // allocate the CXL row
                        std::size_t row_total_size = sizeof(SundialPashaMetadataShared) + table->value_size();
                        char *migrated_row_ptr = reinterpret_cast<char *>(CXLMemory::cxlalloc_malloc_wrapper(row_total_size));
                        char *migrated_row_value_ptr = migrated_row_ptr + sizeof(SundialPashaMetadataShared);
                        SundialPashaMetadataShared *migrated_row_meta = reinterpret_cast<SundialPashaMetadataShared *>(migrated_row_ptr);
                        new(migrated_row_meta) SundialPashaMetadataShared();

                        // take the CXL latch
                        migrated_row_meta->lock();

                        // copy metadata
                        migrated_row_meta->wts = lmeta->wts;
                        migrated_row_meta->rts = lmeta->rts;
                        migrated_row_meta->owner = lmeta->owner;

                        // copy data
                        std::memcpy(migrated_row_value_ptr, local_data, table->value_size());

                        // insert into CXL hash tables
                        CCHashTable *target_cxl_table = cxl_tbl_vecs[table->tableID()][table->partitionID()];
                        ret = target_cxl_table->insert(plain_key, migrated_row_ptr);
                        DCHECK(ret == true);
                        CCSet *row_set = target_cxl_table->search(plain_key);
                        DCHECK(row_set != nullptr);

                        // mark the local row as migrated
                        lmeta->migrated_row = migrated_row_ptr;
                        lmeta->is_migrated = true;

                        // release the CXL latch
                        migrated_row_meta->unlock();

                        ret = true;
                } else {
                        // do nothing
                        ret = false;
                }
		lmeta->unlock();
		return ret;
	}

        static bool move_from_shared_region_to_partition(ITable *table, const std::tuple<MetaDataType *, void *> &row)
	{
                return true;
	}

    public:
	static constexpr int LOCK_BIT_OFFSET = 63;
	static constexpr uint64_t LOCK_BIT_MASK = 0x1ull;
    private:
        static constexpr uint64_t cxl_hashtable_bkt_cnt = 1000;

        std::size_t coordinator_id;
        std::size_t coordinator_num;
        std::size_t table_num_per_partition;
        std::size_t partition_num_per_host;

        CCHashTable *cxl_hashtables;
        std::vector<std::vector<CCHashTable *> > cxl_tbl_vecs;
        std::atomic<uint64_t> init_finished;
};

extern SundialPashaHelper global_helper;

} // namespace star

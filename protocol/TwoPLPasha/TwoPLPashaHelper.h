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
#include "core/CXLTable.h"
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
                , is_valid(false)
                , is_migrated(false)
                , migrated_row(nullptr)
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

	uint64_t tid{ 0 };

        bool is_valid{ false };

        bool is_migrated{ false };
        char *migrated_row{ nullptr };
};

struct TwoPLPashaMetadataShared {
        TwoPLPashaMetadataShared()
                : tid(0)
                , scc_meta(0)
                , flags(0)
                , ref_cnt(0)
        {
                // this spinlock will be shared between multiple processes
                pthread_spin_init(&latch, PTHREAD_PROCESS_SHARED);
        }

        static constexpr int valid_flag_index = 0;
        static constexpr int is_prev_key_real_flag_index = 1;
        static constexpr int is_next_key_real_flag_index = 2;

        void lock()
	{
                pthread_spin_lock(&latch);
	}

	void unlock()
	{
		pthread_spin_unlock(&latch);
	}

        bool get_flag(int flag_index) {
                return (flags & (1 << flag_index)) != 0;
        }

        void set_flag(int flag_index) {
                flags |= (1 << flag_index);  // Set the specific bit to 1
        }

        void clear_flag(int flag_index) {
                flags &= ~(1 << flag_index);  // Clear the specific bit to 0
        }

        uint64_t tid{ 0 };

        pthread_spinlock_t latch;

        // software cache-coherence metadata
        uint16_t scc_meta{ 0 };         // directly embed it here to avoid extra cxlalloc_malloc

        // is_valid, is_next_key_real, is_prev_key_real
        uint8_t flags{ 0 };

        // multi-host transaction accessing a cxl row would increase its reference count by 1
        // a migrated row can only be moved out if its ref_cnt == 0
        // TODO: remove the need for ref_cnt
        uint8_t ref_cnt{ 0 };

        // migration policy metadata
        char migration_policy_meta[MigrationManager::migration_policy_meta_size];         // directly embed it here to avoid extra cxlalloc_malloc
};

uint64_t TwoPLPashaMetadataLocalInit(bool is_tuple_valid);

class TwoPLPashaHelper {
    public:
	using MetaDataType = std::atomic<uint64_t>;

        TwoPLPashaHelper(std::size_t coordinator_id, std::vector<std::vector<CXLTableBase *> > &cxl_tbl_vecs)
                : coordinator_id(coordinator_id)
                , cxl_tbl_vecs(cxl_tbl_vecs)
        {
        }

	uint64_t read(const std::tuple<MetaDataType *, void *> &row, void *dest, std::size_t size, std::atomic<uint64_t> &local_cxl_access)
	{
                MetaDataType &meta = *std::get<0>(row);
                TwoPLPashaMetadataLocal *lmeta = reinterpret_cast<TwoPLPashaMetadataLocal *>(meta.load());
                uint64_t tid_ = 0;

                lmeta->lock();
                CHECK(lmeta->is_valid == true);
                if (lmeta->is_migrated == false) {
		        void *src = std::get<1>(row);
		        std::memcpy(dest, src, size);
                        tid_ = lmeta->tid;
                } else {
                        // statistics
                        local_cxl_access.fetch_add(1);

                        TwoPLPashaMetadataShared *smeta = reinterpret_cast<TwoPLPashaMetadataShared *>(lmeta->migrated_row);
                        void *src = lmeta->migrated_row + sizeof(TwoPLPashaMetadataShared);
                        smeta->lock();
                        CHECK(smeta->get_flag(TwoPLPashaMetadataShared::valid_flag_index) == true);
                        scc_manager->do_read(&smeta->scc_meta, coordinator_id, dest, src, size);
                        tid_ = smeta->tid;
                        smeta->unlock();
                }
                lmeta->unlock();

		return remove_lock_bit(tid_);
	}

        uint64_t remote_read(char *row, void *dest, std::size_t size)
	{
		TwoPLPashaMetadataShared *smeta = reinterpret_cast<TwoPLPashaMetadataShared *>(row);
                void *src = row + sizeof(TwoPLPashaMetadataShared);
                uint64_t tid_ = 0;

		smeta->lock();
                CHECK(smeta->get_flag(TwoPLPashaMetadataShared::valid_flag_index) == true);
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
                CHECK(lmeta->is_valid == true);
                if (lmeta->is_migrated == false) {
                        void *data_ptr = std::get<1>(row);
                        std::memcpy(data_ptr, value, value_size);
                } else {
                        TwoPLPashaMetadataShared *smeta = reinterpret_cast<TwoPLPashaMetadataShared *>(lmeta->migrated_row);
                        void *data_ptr = lmeta->migrated_row + sizeof(TwoPLPashaMetadataShared);
                        smeta->lock();
                        CHECK(smeta->get_flag(TwoPLPashaMetadataShared::valid_flag_index) == true);
                        scc_manager->do_write(&smeta->scc_meta, coordinator_id, data_ptr, value, value_size);
                        smeta->unlock();
                }
		lmeta->unlock();
	}

        void remote_update(char *row, const void *value, std::size_t value_size)
	{
		TwoPLPashaMetadataShared *smeta = reinterpret_cast<TwoPLPashaMetadataShared *>(row);
                void *data_ptr = row + sizeof(TwoPLPashaMetadataShared);

		smeta->lock();
                CHECK(smeta->get_flag(TwoPLPashaMetadataShared::valid_flag_index) == true);
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
                uint64_t old_value = 0, new_value = 0;

                lmeta->lock();
                if (lmeta->is_migrated == false) {
                        if (lmeta->is_valid == false) {
                                success = false;
                                goto out_unlock_lmeta;
                        }

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

                        if (smeta->get_flag(TwoPLPashaMetadataShared::valid_flag_index) == false) {
                                smeta->unlock();
                                success = false;
                                goto out_unlock_lmeta;
                        }

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
		TwoPLPashaMetadataShared *smeta = reinterpret_cast<TwoPLPashaMetadataShared *>(row);
                uint64_t old_value = 0, new_value = 0;

		smeta->lock();

                // because this function is only called by remote point queries,
                // which are assumed to always succeed,
                // so this assertion should always hold
                CHECK(smeta->get_flag(TwoPLPashaMetadataShared::valid_flag_index) == true);

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

        static uint64_t remote_read_lock_and_inc_ref_cnt(char *row, bool &success)
	{
		TwoPLPashaMetadataShared *smeta = reinterpret_cast<TwoPLPashaMetadataShared *>(row);
                uint64_t old_value = 0, new_value = 0;

		smeta->lock();
                if (smeta->get_flag(TwoPLPashaMetadataShared::valid_flag_index) == false) {
                        success = false;
                        smeta->unlock();
                        return remove_lock_bit(old_value);
                }

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

                // increase reference counting only if we get the lock
                smeta->ref_cnt++;

                smeta->unlock();

		return remove_lock_bit(old_value);
	}

	static uint64_t write_lock(std::atomic<uint64_t> &meta, bool &success)
	{
                TwoPLPashaMetadataLocal *lmeta = reinterpret_cast<TwoPLPashaMetadataLocal *>(meta.load());
                uint64_t old_value = 0, new_value = 0;

                lmeta->lock();
                if (lmeta->is_migrated == false) {
                        if (lmeta->is_valid == false) {
                                success = false;
                                goto out_unlock_lmeta;
                        }

                        old_value = lmeta->tid;

                        // can we get the lock?
                        if (is_read_locked(old_value) || is_write_locked(old_value)) {
                                success = false;
                                goto out_unlock_lmeta;
                        }

                        // OK, we can get the lock
                        new_value = old_value + (WRITE_LOCK_BIT_MASK << WRITE_LOCK_BIT_OFFSET);
                        lmeta->tid = new_value;
                        success = true;
                } else {
                        TwoPLPashaMetadataShared *smeta = reinterpret_cast<TwoPLPashaMetadataShared *>(lmeta->migrated_row);
                        smeta->lock();
                        if (smeta->get_flag(TwoPLPashaMetadataShared::valid_flag_index) == false) {
                                smeta->unlock();
                                success = false;
                                goto out_unlock_lmeta;
                        }

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
		TwoPLPashaMetadataShared *smeta = reinterpret_cast<TwoPLPashaMetadataShared *>(row);
                uint64_t old_value = 0, new_value = 0;

		smeta->lock();

                // because this function is only called by remote point queries,
                // which are assumed to always succeed,
                // so this assertion should always hold
                CHECK(smeta->get_flag(TwoPLPashaMetadataShared::valid_flag_index) == true);

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

        static uint64_t remote_write_lock_and_inc_ref_cnt(char *row, bool &success)
	{
		TwoPLPashaMetadataShared *smeta = reinterpret_cast<TwoPLPashaMetadataShared *>(row);
                uint64_t old_value = 0, new_value = 0;

		smeta->lock();
                if (smeta->get_flag(TwoPLPashaMetadataShared::valid_flag_index) == false) {
                        success = false;
                        smeta->unlock();
                        return remove_lock_bit(old_value);
                }

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

                // increase reference counting only if we get the lock
                smeta->ref_cnt++;

                smeta->unlock();

		return remove_lock_bit(old_value);
	}

	static void read_lock_release(std::atomic<uint64_t> &meta)
	{
                TwoPLPashaMetadataLocal *lmeta = reinterpret_cast<TwoPLPashaMetadataLocal *>(meta.load());
                uint64_t old_value = 0, new_value = 0;

                lmeta->lock();
                if (lmeta->is_migrated == false) {
                        CHECK(lmeta->is_valid == true);
                        old_value = lmeta->tid;
			DCHECK(is_read_locked(old_value));
			DCHECK(!is_write_locked(old_value));
			new_value = old_value - (1ull << READ_LOCK_BIT_OFFSET);
                        lmeta->tid = new_value;
                } else {
                        TwoPLPashaMetadataShared *smeta = reinterpret_cast<TwoPLPashaMetadataShared *>(lmeta->migrated_row);
                        smeta->lock();
                        CHECK(smeta->get_flag(TwoPLPashaMetadataShared::valid_flag_index) == true);

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
		TwoPLPashaMetadataShared *smeta = reinterpret_cast<TwoPLPashaMetadataShared *>(row);
                uint64_t old_value = 0, new_value = 0;

		smeta->lock();
                CHECK(smeta->get_flag(TwoPLPashaMetadataShared::valid_flag_index) == true);

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
                uint64_t old_value = 0, new_value = 0;

                lmeta->lock();
                if (lmeta->is_migrated == false) {
                        CHECK(lmeta->is_valid == true);
                        old_value = lmeta->tid;
                        DCHECK(!is_read_locked(old_value));
                        DCHECK(is_write_locked(old_value));
                        new_value = old_value - (1ull << WRITE_LOCK_BIT_OFFSET);
                        lmeta->tid = new_value;
                } else {
                        TwoPLPashaMetadataShared *smeta = reinterpret_cast<TwoPLPashaMetadataShared *>(lmeta->migrated_row);
                        smeta->lock();
                        CHECK(smeta->get_flag(TwoPLPashaMetadataShared::valid_flag_index) == true);

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
		TwoPLPashaMetadataShared *smeta = reinterpret_cast<TwoPLPashaMetadataShared *>(row);
                uint64_t old_value = 0, new_value = 0;

		smeta->lock();
                CHECK(smeta->get_flag(TwoPLPashaMetadataShared::valid_flag_index) == true);

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
                uint64_t old_value = 0;

                lmeta->lock();
                if (lmeta->is_migrated == false) {
                        CHECK(lmeta->is_valid == true);
                        old_value = lmeta->tid;
                        DCHECK(!is_read_locked(old_value));
                        DCHECK(is_write_locked(old_value));
                        DCHECK(!is_read_locked(new_value));
                        DCHECK(!is_write_locked(new_value));
                        lmeta->tid = new_value;
                } else {
                        TwoPLPashaMetadataShared *smeta = reinterpret_cast<TwoPLPashaMetadataShared *>(lmeta->migrated_row);
                        smeta->lock();
                        CHECK(smeta->get_flag(TwoPLPashaMetadataShared::valid_flag_index) == true);

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
		TwoPLPashaMetadataShared *smeta = reinterpret_cast<TwoPLPashaMetadataShared *>(row);
                uint64_t old_value = 0;

		smeta->lock();
                CHECK(smeta->get_flag(TwoPLPashaMetadataShared::valid_flag_index) == true);

                old_value = smeta->tid;
                DCHECK(!is_read_locked(old_value));
                DCHECK(is_write_locked(old_value));
                DCHECK(!is_read_locked(new_value));
                DCHECK(!is_write_locked(new_value));
                smeta->tid = new_value;

                smeta->unlock();
	}

        static void modify_tuple_valid_bit(std::atomic<uint64_t> &meta, bool is_valid)
        {
                TwoPLPashaMetadataLocal *lmeta = reinterpret_cast<TwoPLPashaMetadataLocal *>(meta.load());

                lmeta->lock();
                if (lmeta->is_migrated == true) {
                        TwoPLPashaMetadataShared *smeta = reinterpret_cast<TwoPLPashaMetadataShared *>(lmeta->migrated_row);
                        smeta->lock();
                        CHECK(smeta->get_flag(TwoPLPashaMetadataShared::valid_flag_index) == !is_valid);
                        if (is_valid == true) {
                                smeta->set_flag(TwoPLPashaMetadataShared::valid_flag_index);
                        } else {
                                smeta->clear_flag(TwoPLPashaMetadataShared::valid_flag_index);
                        }
                        smeta->unlock();
                } else {
                        CHECK(lmeta->is_valid == !is_valid);
                        lmeta->is_valid = is_valid;
                }
                lmeta->unlock();
        }

        static void remote_modify_tuple_valid_bit(char *row, bool is_valid)
        {
                TwoPLPashaMetadataShared *smeta = reinterpret_cast<TwoPLPashaMetadataShared *>(row);

		smeta->lock();
                CHECK(smeta->ref_cnt > 0);
                CHECK(smeta->get_flag(TwoPLPashaMetadataShared::valid_flag_index) == !is_valid);
                if (is_valid == true) {
                        smeta->set_flag(TwoPLPashaMetadataShared::valid_flag_index);
                } else {
                        smeta->clear_flag(TwoPLPashaMetadataShared::valid_flag_index);
                }
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

        void commit_pasha_metadata_init()
        {
                init_finished.store(1, std::memory_order_release);
        }

        void wait_for_pasha_metadata_init()
        {
                while (init_finished.load(std::memory_order_acquire) == 0);
        }

        CXLTableBase *get_cxl_table(std::size_t table_id, std::size_t partition_id)
        {
                return cxl_tbl_vecs[table_id][partition_id];
        }

        // used for remote point queries
        char *get_migrated_row(std::size_t table_id, std::size_t partition_id, const void *key, bool inc_ref_cnt)
        {
                CXLTableBase *target_cxl_table = cxl_tbl_vecs[table_id][partition_id];
                char *migrated_row = reinterpret_cast<char *>(target_cxl_table->search(key));
                void *migration_policy_meta = nullptr;

                if (migrated_row != nullptr) {
                        TwoPLPashaMetadataShared *smeta = reinterpret_cast<TwoPLPashaMetadataShared *>(migrated_row);
                        smeta->lock();
                        if (smeta->get_flag(TwoPLPashaMetadataShared::valid_flag_index) == true) {
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

        // used for remote point queries
        void release_migrated_row(std::size_t table_id, std::size_t partition_id, const void *key)
        {
                CXLTableBase *target_cxl_table = cxl_tbl_vecs[table_id][partition_id];
                char *migrated_row = reinterpret_cast<char *>(target_cxl_table->search(key));
                CHECK(migrated_row != nullptr);

                TwoPLPashaMetadataShared *smeta = reinterpret_cast<TwoPLPashaMetadataShared *>(migrated_row);
                smeta->lock();
                CHECK(smeta->ref_cnt > 0);
                smeta->ref_cnt--;
                smeta->unlock();
        }

        // used for remote scan
        static void decrease_reference_count_via_ptr(void *cxl_row)
        {
                TwoPLPashaMetadataShared *smeta = reinterpret_cast<TwoPLPashaMetadataShared *>(cxl_row);
                smeta->lock();
                CHECK(smeta->ref_cnt > 0);
                smeta->ref_cnt--;
                smeta->unlock();
        }

        // used for remote point queries
        bool remove_migrated_row(std::size_t table_id, std::size_t partition_id, const void *key)
        {
                CXLTableBase *target_cxl_table = cxl_tbl_vecs[table_id][partition_id];
                return target_cxl_table->remove(key, nullptr);
        }

        bool move_from_hashmap_to_shared_region(ITable *table, const void *key, const std::tuple<MetaDataType *, void *> &row, bool inc_ref_cnt, void *&migration_policy_meta)
	{
                MetaDataType &meta = *std::get<0>(row);
                TwoPLPashaMetadataLocal *lmeta = reinterpret_cast<TwoPLPashaMetadataLocal *>(meta.load());
                void *local_data = std::get<1>(row);
                bool move_in_success = false;
                bool ret = false;

		lmeta->lock();
                if (lmeta->is_migrated == false) {
                        // allocate the CXL row
                        std::size_t row_total_size = sizeof(TwoPLPashaMetadataShared) + table->value_size();
                        char *migrated_row_ptr = reinterpret_cast<char *>(cxl_memory.cxlalloc_malloc_wrapper(row_total_size,
                                CXLMemory::DATA_ALLOCATION, sizeof(TwoPLPashaMetadataShared), table->value_size()));
                        char *migrated_row_value_ptr = migrated_row_ptr + sizeof(TwoPLPashaMetadataShared);
                        TwoPLPashaMetadataShared *smeta = reinterpret_cast<TwoPLPashaMetadataShared *>(migrated_row_ptr);
                        new(smeta) TwoPLPashaMetadataShared();

                        // init migration policy metadata
                        migration_manager->init_migration_policy_metadata(&smeta->migration_policy_meta, table, key, row, sizeof(TwoPLPashaMetadataShared));
                        migration_policy_meta = smeta->migration_policy_meta;

                        // init software cache-coherence metadata
                        scc_manager->init_scc_metadata(&smeta->scc_meta, coordinator_id);

                        // take the CXL latch
                        smeta->lock();

                        // copy metadata
                        if (lmeta->is_valid == true) {
                                smeta->set_flag(TwoPLPashaMetadataShared::valid_flag_index);
                        } else {
                                smeta->clear_flag(TwoPLPashaMetadataShared::valid_flag_index);
                        }
                        smeta->tid = lmeta->tid;

                        // copy data
                        scc_manager->do_write(&smeta->scc_meta, coordinator_id, migrated_row_value_ptr, local_data, table->value_size());

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
                                TwoPLPashaMetadataShared *smeta = reinterpret_cast<TwoPLPashaMetadataShared *>(lmeta->migrated_row);
                                smeta->lock();
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
                bool move_in_success = false;
                bool ret = false;

                auto move_in_processor = [&](const void *prev_key, void *prev_meta, void *prev_data, const void *cur_key, void *cur_meta, void *cur_data, const void *next_key, void *next_meta, void *next_data) {
                        auto prev_lmeta = reinterpret_cast<TwoPLPashaMetadataLocal *>(prev_meta);
                        auto cur_lmeta = reinterpret_cast<TwoPLPashaMetadataLocal *>(cur_meta);
                        auto next_lmeta = reinterpret_cast<TwoPLPashaMetadataLocal *>(next_meta);

                        bool is_next_key_migrated = false, next_key_exist = false;
                        bool is_prev_key_migrated = false, prev_key_exist = false;

                        // check if the previous tuple is migrated
                        // and update its next-key information
                        if (prev_lmeta != nullptr) {
                                prev_lmeta->lock();
                                if (prev_lmeta->is_migrated == true) {
                                        is_prev_key_migrated = true;
                                }
                                prev_lmeta->unlock();
                                prev_key_exist = true;
                        }

                        // check if the next tuple is migrated
                        // and update its prev-key information
                        if (next_lmeta != nullptr) {
                                next_lmeta->lock();
                                if (next_lmeta->is_migrated == true) {
                                        is_next_key_migrated = true;
                                }
                                next_lmeta->unlock();
                                next_key_exist = true;
                        }

                        // check if the current tuple is migrated
                        // if yes, do the migration and update the next-key information
                        cur_lmeta->lock();
                        if (cur_lmeta->is_migrated == false) {
                                // allocate the CXL row
                                std::size_t row_total_size = sizeof(TwoPLPashaMetadataShared) + table->value_size();
                                char *migrated_row_ptr = reinterpret_cast<char *>(cxl_memory.cxlalloc_malloc_wrapper(row_total_size,
                                                CXLMemory::DATA_ALLOCATION, sizeof(TwoPLPashaMetadataShared), table->value_size()));
                                char *migrated_row_value_ptr = migrated_row_ptr + sizeof(TwoPLPashaMetadataShared);
                                TwoPLPashaMetadataShared *cur_smeta = reinterpret_cast<TwoPLPashaMetadataShared *>(migrated_row_ptr);
                                new(cur_smeta) TwoPLPashaMetadataShared();

                                // init migration policy metadata
                                migration_manager->init_migration_policy_metadata(&cur_smeta->migration_policy_meta, table, key, row, sizeof(TwoPLPashaMetadataShared));
                                migration_policy_meta = cur_smeta->migration_policy_meta;

                                // init software cache-coherence metadata
                                scc_manager->init_scc_metadata(&cur_smeta->scc_meta, coordinator_id);

                                // take the CXL latch
                                cur_smeta->lock();

                                // copy metadata
                                if (cur_lmeta->is_valid == true) {
                                        cur_smeta->set_flag(TwoPLPashaMetadataShared::valid_flag_index);
                                } else {
                                        cur_smeta->clear_flag(TwoPLPashaMetadataShared::valid_flag_index);
                                }
                                cur_smeta->tid = cur_lmeta->tid;

                                // copy data
                                scc_manager->do_write(&cur_smeta->scc_meta, coordinator_id, migrated_row_value_ptr, cur_data, table->value_size());

                                // increase the reference count for the requesting host
                                if (inc_ref_cnt == true) {
                                        cur_smeta->ref_cnt++;
                                }

                                // update the next-key information
                                if (is_next_key_migrated == true) {
                                        cur_smeta->set_flag(TwoPLPashaMetadataShared::is_next_key_real_flag_index);
                                } else {
                                        cur_smeta->clear_flag(TwoPLPashaMetadataShared::is_next_key_real_flag_index);
                                }

                                // update the prev-key information
                                if (is_prev_key_migrated == true) {
                                        cur_smeta->set_flag(TwoPLPashaMetadataShared::is_prev_key_real_flag_index);
                                } else {
                                        cur_smeta->clear_flag(TwoPLPashaMetadataShared::is_prev_key_real_flag_index);
                                }

                                // insert into the corresponding CXL table
                                CXLTableBase *target_cxl_table = cxl_tbl_vecs[table->tableID()][table->partitionID()];
                                ret = target_cxl_table->insert(key, migrated_row_ptr);
                                CHECK(ret == true);

                                // mark the local row as migrated
                                cur_lmeta->migrated_row = migrated_row_ptr;
                                cur_lmeta->is_migrated = true;

                                // release the CXL latch
                                cur_smeta->unlock();

                                // lazily update the next-key information for the previous key
                                if (prev_lmeta != nullptr) {
                                        prev_lmeta->lock();
                                        if (prev_lmeta->is_migrated == true) {
                                                auto prev_smeta = reinterpret_cast<TwoPLPashaMetadataShared *>(prev_lmeta->migrated_row);
                                                prev_smeta->lock();
                                                prev_smeta->set_flag(TwoPLPashaMetadataShared::is_next_key_real_flag_index);
                                                prev_smeta->unlock();
                                        }
                                        prev_lmeta->unlock();
                                }

                                // lazily update the next-key information for the previous key
                                if (next_lmeta != nullptr) {
                                        next_lmeta->lock();
                                        if (next_lmeta->is_migrated == true) {
                                                TwoPLPashaMetadataShared *next_smeta = reinterpret_cast<TwoPLPashaMetadataShared *>(next_lmeta->migrated_row);
                                                next_smeta->lock();
                                                next_smeta->set_flag(TwoPLPashaMetadataShared::is_prev_key_real_flag_index);
                                                next_smeta->unlock();
                                        }
                                        next_lmeta->unlock();
                                }

                                move_in_success = true;
                        } else {
                                // lazily update the next-key information for the previous key
                                if (prev_lmeta != nullptr) {
                                        prev_lmeta->lock();
                                        if (prev_lmeta->is_migrated == true) {
                                                auto prev_smeta = reinterpret_cast<TwoPLPashaMetadataShared *>(prev_lmeta->migrated_row);
                                                prev_smeta->lock();
                                                prev_smeta->set_flag(TwoPLPashaMetadataShared::is_next_key_real_flag_index);
                                                prev_smeta->unlock();
                                        }
                                        prev_lmeta->unlock();
                                }

                                // lazily update the next-key information for the previous key
                                if (next_lmeta != nullptr) {
                                        next_lmeta->lock();
                                        if (next_lmeta->is_migrated == true) {
                                                TwoPLPashaMetadataShared *next_smeta = reinterpret_cast<TwoPLPashaMetadataShared *>(next_lmeta->migrated_row);
                                                next_smeta->lock();
                                                next_smeta->set_flag(TwoPLPashaMetadataShared::is_prev_key_real_flag_index);
                                                next_smeta->unlock();
                                        }
                                        next_lmeta->unlock();
                                }

                                if (inc_ref_cnt == true) {
                                        // increase the reference count for the requesting host, even if it is already migrated
                                        TwoPLPashaMetadataShared *cur_smeta = reinterpret_cast<TwoPLPashaMetadataShared *>(cur_lmeta->migrated_row);
                                        cur_smeta->lock();
                                        cur_smeta->ref_cnt++;
                                        cur_smeta->unlock();
                                }
                                move_in_success = false;

                                // the next-key information should already been updated
                        }
                        cur_lmeta->unlock();
		};

                // update next-key information
                ret = table->search_and_update_next_key_info(key, move_in_processor);

		return ret && move_in_success;
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
		TwoPLPashaMetadataLocal *lmeta = reinterpret_cast<TwoPLPashaMetadataLocal *>(meta.load());
                void *local_data = std::get<1>(row);
                bool ret = false;

                lmeta->lock();
                if (lmeta->is_migrated == true) {
                        TwoPLPashaMetadataShared *smeta = reinterpret_cast<TwoPLPashaMetadataShared *>(lmeta->migrated_row);
                        char *migrated_row_value = lmeta->migrated_row + sizeof(TwoPLPashaMetadataShared);

                        // take the CXL latch
                        smeta->lock();

                        // reference count > 0, cannot move out the tuple
                        if (smeta->ref_cnt > 0) {
                                smeta->unlock();
                                lmeta->unlock();
                                return false;
                        }

                        // copy metadata back
                        lmeta->is_valid = smeta->get_flag(TwoPLPashaMetadataShared::valid_flag_index);
                        lmeta->tid = smeta->tid;

                        // copy data back
                        scc_manager->do_read(&smeta->scc_meta, coordinator_id, local_data, migrated_row_value, table->value_size());

                        // set the migrated row as invalid
                        smeta->clear_flag(TwoPLPashaMetadataShared::valid_flag_index);

                        // remove from CXL index
                        CXLTableBase *target_cxl_table = cxl_tbl_vecs[table->tableID()][table->partitionID()];
                        ret = target_cxl_table->remove(key, lmeta->migrated_row);
                        CHECK(ret == true);

                        // mark the local row as not migrated
                        lmeta->migrated_row = nullptr;
                        lmeta->is_migrated = false;

                        // free the CXL row
                        // TODO: register EBR
                        std::size_t row_total_size = sizeof(TwoPLPashaMetadataShared) + table->value_size();
                        cxl_memory.cxlalloc_free_wrapper(smeta, row_total_size,
                                CXLMemory::DATA_FREE, sizeof(TwoPLPashaMetadataShared), table->value_size());

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
                bool move_out_success = false;
                bool ret = false;

                auto move_out_processor = [&](const void *prev_key, void *prev_meta, void *prev_data, const void *cur_key, void *cur_meta, void *cur_data, const void *next_key, void *next_meta, void *next_data) {
                        auto prev_lmeta = reinterpret_cast<TwoPLPashaMetadataLocal *>(prev_meta);
                        auto cur_lmeta = reinterpret_cast<TwoPLPashaMetadataLocal *>(cur_meta);
                        auto next_lmeta = reinterpret_cast<TwoPLPashaMetadataLocal *>(next_meta);

                        bool is_cur_tuple_moved_out = false;

                        // eagerly update the next-key information for the previous tuple
                        if (prev_lmeta != nullptr) {
                                prev_lmeta->lock();
                                if (prev_lmeta->is_migrated == true) {
                                        auto prev_smeta = reinterpret_cast<TwoPLPashaMetadataShared *>(prev_lmeta->migrated_row);
                                        prev_smeta->lock();
                                        prev_smeta->clear_flag(TwoPLPashaMetadataShared::is_next_key_real_flag_index);
                                        prev_smeta->unlock();
                                }
                                prev_lmeta->unlock();
                        }

                        // eagerly update the prev-key information for the next tuple
                        if (next_lmeta != nullptr) {
                                next_lmeta->lock();
                                if (next_lmeta->is_migrated == true) {
                                        auto next_smeta = reinterpret_cast<TwoPLPashaMetadataShared *>(next_lmeta->migrated_row);
                                        next_smeta->lock();
                                        next_smeta->clear_flag(TwoPLPashaMetadataShared::is_prev_key_real_flag_index);
                                        next_smeta->unlock();
                                }
                                next_lmeta->unlock();
                        }

                        cur_lmeta->lock();
                        CHECK(cur_lmeta->is_valid = true);
                        if (cur_lmeta->is_migrated == true) {
                                TwoPLPashaMetadataShared *cur_smeta = reinterpret_cast<TwoPLPashaMetadataShared *>(cur_lmeta->migrated_row);
                                char *migrated_row_value = cur_lmeta->migrated_row + sizeof(TwoPLPashaMetadataShared);

                                // take the CXL latch
                                cur_smeta->lock();

                                // reference count > 0, cannot move out the tuple -> early return
                                if (cur_smeta->ref_cnt > 0) {
                                        cur_smeta->unlock();
                                        cur_lmeta->unlock();
                                        move_out_success = false;
                                        return;
                                }

                                // copy metadata back
                                cur_lmeta->is_valid = cur_smeta->get_flag(TwoPLPashaMetadataShared::valid_flag_index);
                                cur_lmeta->tid = cur_smeta->tid;

                                // copy data back
                                scc_manager->do_read(&cur_smeta->scc_meta, coordinator_id, cur_data, migrated_row_value, table->value_size());

                                // set the migrated row as invalid
                                cur_smeta->clear_flag(TwoPLPashaMetadataShared::valid_flag_index);

                                // mark the local row as not migrated
                                cur_lmeta->migrated_row = nullptr;
                                cur_lmeta->is_migrated = false;

                                // free the CXL row
                                // TODO: register EBR
                                std::size_t row_total_size = sizeof(TwoPLPashaMetadataShared) + table->value_size();
                                cxl_memory.cxlalloc_free_wrapper(cur_smeta, row_total_size,
                                        CXLMemory::DATA_FREE, sizeof(TwoPLPashaMetadataShared), table->value_size());

                                // release the CXL latch
                                cur_smeta->unlock();

                                // remove the current-key from the CXL index
                                // it is safe to do so because there is no concurrent data move in/out
                                CXLTableBase *target_cxl_table = cxl_tbl_vecs[table->tableID()][table->partitionID()];
                                ret = target_cxl_table->remove(key, nullptr);
                                CHECK(ret == true);
                        } else {
                                CHECK(0);
                        }
                        cur_lmeta->unlock();

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

        bool insert_and_update_next_key_info(ITable *table, const void *key, const void *value, bool require_lock_next_key, ITable::row_entity &next_row_entity)
        {
                auto adjacent_tuples_processor = [&](const void *prev_key, MetaDataType *prev_meta, void *prev_data, const void *next_key, MetaDataType *next_meta, void *next_data) -> bool {
                        TwoPLPashaMetadataLocal *prev_lmeta = nullptr, *next_lmeta = nullptr;
                        if (prev_meta != nullptr) {
                                prev_lmeta = reinterpret_cast<TwoPLPashaMetadataLocal *>(prev_meta->load());
                        }
                        if (next_meta != nullptr) {
                                next_lmeta = reinterpret_cast<TwoPLPashaMetadataLocal *>(next_meta->load());
                        }

                        if (require_lock_next_key == true) {
                                // try to acquire the write lock of the next key
                                CHECK(next_meta != nullptr);
                                std::atomic<uint64_t> &meta = *reinterpret_cast<std::atomic<uint64_t> *>(next_meta);
                                bool lock_success = false;
                                TwoPLPashaHelper::write_lock(meta, lock_success);
                                if (lock_success == true) {
                                        ITable::row_entity next_row(next_key, table->key_size(), &meta, next_data, table->value_size());
                                        next_row_entity = next_row;

                                        // update the next key information for the previous key
                                        if (prev_lmeta != nullptr) {
                                                prev_lmeta->lock();
                                                if (prev_lmeta->is_migrated == true) {
                                                        auto prev_smeta = reinterpret_cast<TwoPLPashaMetadataShared *>(prev_lmeta->migrated_row);
                                                        prev_smeta->lock();
                                                        prev_smeta->clear_flag(TwoPLPashaMetadataShared::is_next_key_real_flag_index);
                                                        prev_smeta->unlock();
                                                }
                                                prev_lmeta->unlock();
                                        }

                                        // update the previous key information for the next key
                                        if (next_lmeta != nullptr) {
                                                next_lmeta->lock();
                                                if (next_lmeta->is_migrated == true) {
                                                        auto next_smeta = reinterpret_cast<TwoPLPashaMetadataShared *>(next_lmeta->migrated_row);
                                                        next_smeta->lock();
                                                        next_smeta->clear_flag(TwoPLPashaMetadataShared::is_prev_key_real_flag_index);
                                                        next_smeta->unlock();
                                                }
                                                next_lmeta->unlock();
                                        }
                                }
                                return lock_success;
                        } else {
                                // update the next key information for the previous key
                                if (prev_lmeta != nullptr) {
                                        prev_lmeta->lock();
                                        if (prev_lmeta->is_migrated == true) {
                                                auto prev_smeta = reinterpret_cast<TwoPLPashaMetadataShared *>(prev_lmeta->migrated_row);
                                                prev_smeta->lock();
                                                prev_smeta->clear_flag(TwoPLPashaMetadataShared::is_next_key_real_flag_index);
                                                prev_smeta->unlock();
                                        }
                                        prev_lmeta->unlock();
                                }

                                // update the previous key information for the next key
                                if (next_lmeta != nullptr) {
                                        next_lmeta->lock();
                                        if (next_lmeta->is_migrated == true) {
                                                auto next_smeta = reinterpret_cast<TwoPLPashaMetadataShared *>(next_lmeta->migrated_row);
                                                next_smeta->lock();
                                                next_smeta->clear_flag(TwoPLPashaMetadataShared::is_prev_key_real_flag_index);
                                                next_smeta->unlock();
                                        }
                                        next_lmeta->unlock();
                                }
                                return true;
                        }
                };

                // insert a placeholder and update adjacent tuple information if necessary
                return table->insert_and_process_adjacent_tuples(key, value, adjacent_tuples_processor, true);
        }

        bool delete_and_update_next_key_info(ITable *table, const void *key, bool is_local_delete, bool &need_move_out_from_migration_tracker, void *&migration_policy_meta)
	{
                 auto adjacent_tuples_processor = [&](const void *prev_key, void *prev_meta, void *prev_data, const void *cur_key, void *cur_meta, void *cur_data, const void *next_key, void *next_meta, void *next_data) -> bool {
                        auto prev_lmeta = reinterpret_cast<TwoPLPashaMetadataLocal *>(prev_meta);
                        auto cur_lmeta = reinterpret_cast<TwoPLPashaMetadataLocal *>(cur_meta);
                        auto next_lmeta = reinterpret_cast<TwoPLPashaMetadataLocal *>(next_meta);

                        bool is_next_key_migrated = false;
                        bool is_prev_key_migrated = false;
                        bool need_remove_from_cxl_index = false;

                        // take the latches of the previous and the next keys
                        if (prev_lmeta != nullptr) {
                                prev_lmeta->lock();
                        }
                        if (next_lmeta != nullptr) {
                                next_lmeta->lock();
                        }

                        // update the next-key information for the previous key
                        if (prev_lmeta != nullptr) {
                                if (prev_lmeta->is_migrated == true) {
                                        auto prev_smeta = reinterpret_cast<TwoPLPashaMetadataShared *>(prev_lmeta->migrated_row);
                                        prev_smeta->lock();
                                        if (next_lmeta != nullptr && next_lmeta->is_migrated == false) {
                                                prev_smeta->clear_flag(TwoPLPashaMetadataShared::is_next_key_real_flag_index);
                                        } else if (next_lmeta != nullptr && next_lmeta->is_migrated == true) {
                                                // We should treat migrated tuples as equal no matter they are valid or not.
                                                // Since we are removing the migrated tuple but keeping the local tuple, we should mark the next key as false.
                                                prev_smeta->clear_flag(TwoPLPashaMetadataShared::is_next_key_real_flag_index);
                                        } else {
                                                prev_smeta->clear_flag(TwoPLPashaMetadataShared::is_next_key_real_flag_index);
                                        }
                                        prev_smeta->unlock();
                                }
                        }

                        // update the prev-key information for next key
                        if (next_lmeta != nullptr) {
                                if (next_lmeta->is_migrated == true) {
                                        auto next_smeta = reinterpret_cast<TwoPLPashaMetadataShared *>(next_lmeta->migrated_row);
                                        next_smeta->lock();
                                        if (prev_lmeta != nullptr && prev_lmeta->is_migrated == false) {
                                                next_smeta->clear_flag(TwoPLPashaMetadataShared::is_prev_key_real_flag_index);
                                        } else if (prev_lmeta != nullptr && prev_lmeta->is_migrated == true) {
                                                // We should treat migrated tuples as equal no matter they are valid or not.
                                                // Since we are removing the migrated tuple but keeping the local tuple, we should mark the previous key as false.
                                                next_smeta->clear_flag(TwoPLPashaMetadataShared::is_prev_key_real_flag_index);
                                        } else {
                                                next_smeta->clear_flag(TwoPLPashaMetadataShared::is_prev_key_real_flag_index);
                                        }
                                        next_smeta->unlock();
                                }
                        }

                        // release the latches of the previous and the next keys
                        if (prev_lmeta != nullptr) {
                                prev_lmeta->unlock();
                        }
                        if (next_lmeta != nullptr) {
                                next_lmeta->unlock();
                        }

                        // mark both the local and the migrated tuples as invalid
                        CHECK(cur_lmeta != nullptr);
                        cur_lmeta->lock();
                        if (cur_lmeta->is_migrated == true) {
                                auto cur_smeta = reinterpret_cast<TwoPLPashaMetadataShared *>(cur_lmeta->migrated_row);
                                cur_smeta->lock();
                                if (is_local_delete == true) {
                                        CHECK(cur_smeta->get_flag(TwoPLPashaMetadataShared::valid_flag_index) == true);
                                        cur_smeta->clear_flag(TwoPLPashaMetadataShared::valid_flag_index);
                                } else {
                                        CHECK(cur_smeta->get_flag(TwoPLPashaMetadataShared::valid_flag_index) == false);
                                }
                                migration_policy_meta = cur_smeta->migration_policy_meta;
                                cur_smeta->unlock();

                                need_remove_from_cxl_index = true;
                                need_move_out_from_migration_tracker = true;

                                cur_lmeta->is_migrated = false;
                        }

                        // local tuple might be invalid here because it can be moved back after being marked as invalid by a remote host
                        cur_lmeta->is_valid = false;
                        cur_lmeta->unlock();

                        // remove the migrated row from the CXL index
                        if (need_remove_from_cxl_index) {
                                bool remove_success = remove_migrated_row(table->tableID(), table->partitionID(), key);
                                CHECK(remove_success == true);
                        }

                        return true;
                };

                // update next key info, mark both the local and the migrated tuples as invalid, and remove the migrated tuple
                bool success = table->remove_and_process_adjacent_tuples(key, adjacent_tuples_processor);
                CHECK(success == true);

                return true;
	}

    public:
	static constexpr int LOCK_BIT_OFFSET = 54;
	static constexpr uint64_t LOCK_BIT_MASK = 0x3ffull;

	static constexpr int READ_LOCK_BIT_OFFSET = 54;
	static constexpr uint64_t READ_LOCK_BIT_MASK = 0x1ffull;

	static constexpr int WRITE_LOCK_BIT_OFFSET = 63;
	static constexpr uint64_t WRITE_LOCK_BIT_MASK = 0x1ull;

    private:
        std::size_t coordinator_id;

        std::vector<std::vector<CXLTableBase *> > &cxl_tbl_vecs;

        std::atomic<uint64_t> init_finished;
};

extern TwoPLPashaHelper *twopl_pasha_global_helper;

} // namespace star

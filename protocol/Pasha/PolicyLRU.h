//
// Created by Yibo Huang on 09/02/24 (Labor Day)!
//

#pragma once

#include <mutex>
#include <list>
#include "stdint.h"

#include "core/Table.h"
#include "common/CXLMemory.h"
#include "protocol/Pasha/MigrationManager.h"

#include <boost/interprocess/offset_ptr.hpp>

namespace star
{

class PolicyLRU : public MigrationManager {
    public:
        struct LRUMeta {
                MigrationManager::migrated_row_entity row_entity;       // TODO: this does not need to be in CXL

                boost::interprocess::offset_ptr<LRUMeta> prev{ nullptr };
                boost::interprocess::offset_ptr<LRUMeta> next{ nullptr };
        };

        class LRUTracker {
            public:
                LRUTracker()
                : head{nullptr}
                , tail{nullptr}
                {
                        // this spinlock will be shared between multiple processes
                        pthread_spin_init(&lru_tracker_lock, PTHREAD_PROCESS_SHARED);
                }

                void lock()
                {
                        pthread_spin_lock(&lru_tracker_lock);
                }

                void unlock()
                {
                        pthread_spin_unlock(&lru_tracker_lock);
                }

                // push back to the tail
                void track(LRUMeta *lru_meta)
                {
                        CHECK(lru_meta->next.get() == nullptr);
                        CHECK(lru_meta->prev.get() == nullptr);

                        if (head.get() == nullptr && tail.get() == nullptr) {
                                head = lru_meta;
                                tail = lru_meta;
                                lru_meta->prev = nullptr;
                                lru_meta->next = nullptr;
                        } else {
                                CHECK(head.get() != nullptr);
                                CHECK(tail.get() != nullptr);
                                CHECK(tail->next.get() == nullptr);
                                CHECK(head->prev.get() == nullptr);

                                tail->next = lru_meta;
                                lru_meta->prev = tail;
                                tail = lru_meta;
                        }
                }

                // remove from the list
                void untrack(LRUMeta *lru_meta)
                {
                        if (head.get() == nullptr && tail.get() == nullptr) {
                                CHECK(0);
                        } else if (head.get() == tail.get()) {
                                CHECK(lru_meta == head.get());
                                CHECK(lru_meta == tail.get());

                                head = nullptr;
                                tail = nullptr;
                        } else {
                                if (lru_meta->prev.get() != nullptr) {
                                        lru_meta->prev->next = lru_meta->next;
                                }
                                if (lru_meta->next.get() != nullptr) {
                                        lru_meta->next->prev = lru_meta->prev;
                                }

                                if (head.get() == lru_meta) {
                                        head = lru_meta->next;
                                }
                                if (tail.get() == lru_meta) {
                                        tail = lru_meta->prev;
                                }
                        }

                        lru_meta->prev = nullptr;
                        lru_meta->next = nullptr;
                }

                // promote to the tail
                void promote(LRUMeta *lru_meta)
                {
                        untrack(lru_meta);
                        track(lru_meta);
                }

                // head is the victim
                LRUMeta *get_next_victim()
                {
                        if (cur_victim == nullptr) {
                                cur_victim = head.get();
                        } else {
                                cur_victim = cur_victim->next.get();
                        }

                        return cur_victim;
                }

                void reset_cur_victim()
                {
                        cur_victim = nullptr;
                }

                void debug_print()
                {
                        LRUMeta *cur_node = head.get();
                        uint64_t i = 0;

                        LOG(INFO) << "LRU list status:";
                        while (true) {
                                if (cur_node == nullptr) {
                                        break;
                                } else {
                                        ITable *table = cur_node->row_entity.table;
                                        const void *key = cur_node->row_entity.key;
                                        LOG(INFO) << "ID = " << i << " key = " << table->get_plain_key(key);
                                }
                                i++;
                                cur_node = cur_node->next.get();
                        }
                }

            private:
                boost::interprocess::offset_ptr<LRUMeta> head{ nullptr };
                boost::interprocess::offset_ptr<LRUMeta> tail{ nullptr };

                LRUMeta *cur_victim = nullptr;

                pthread_spinlock_t lru_tracker_lock;
        };

        PolicyLRU(std::function<bool(ITable *, const void *, const std::tuple<std::atomic<uint64_t> *, void *> &, bool, void *&)> move_from_partition_to_shared_region,
                        std::function<bool(ITable *, const void *, const std::tuple<std::atomic<uint64_t> *, void *> &)> move_from_shared_region_to_partition,
                        std::function<bool(ITable *, const void *, bool, bool &, void *&)> delete_and_update_next_key_info,
                        uint64_t coordinator_id,
                        uint64_t partition_num,
                        const std::string when_to_move_out_str,
                        uint64_t max_migrated_rows_size)
        : MigrationManager(move_from_partition_to_shared_region, move_from_shared_region_to_partition, delete_and_update_next_key_info, when_to_move_out_str)
        , max_migrated_rows_size_per_partition(max_migrated_rows_size)
        {
                CHECK(MigrationManager::migration_policy_meta_size >= sizeof(LRUMeta));
                migrated_row_sizes_of_partitions = new uint64_t[partition_num];
                CHECK(migrated_row_sizes_of_partitions != nullptr);
                memset(migrated_row_sizes_of_partitions, 0, partition_num * sizeof(uint64_t));

                if (coordinator_id == 0) {
                        lru_trackers = reinterpret_cast<LRUTracker *>(cxl_memory.cxlalloc_malloc_wrapper(sizeof(LRUTracker) * partition_num, CXLMemory::INDEX_ALLOCATION));
                        for (int i = 0; i < partition_num; i++) {
                                new(&lru_trackers[i]) LRUTracker();
                        }
                        CXLMemory::commit_shared_data_initialization(CXLMemory::cxl_lru_trackers_root_index, lru_trackers);
                } else {
                        void *tmp = NULL;
                        CXLMemory::wait_and_retrieve_cxl_shared_data(CXLMemory::cxl_lru_trackers_root_index, &tmp);
                        lru_trackers = reinterpret_cast<LRUTracker *>(tmp);
                }
        }

        void init_migration_policy_metadata(void *migration_policy_meta, ITable *table, const void *key, const std::tuple<MetaDataType *, void *> &row) override
        {
                LRUMeta *lru_meta = reinterpret_cast<LRUMeta *>(migration_policy_meta);
                new(lru_meta) LRUMeta();
                lru_meta->row_entity = MigrationManager::migrated_row_entity(table, key, row);
        }

        void access_row(void *migration_policy_meta, uint64_t partition_id) override
        {
                LRUTracker &lru_tracker = lru_trackers[partition_id];
                uint64_t &cur_size = migrated_row_sizes_of_partitions[partition_id];
                LRUMeta *lru_meta = reinterpret_cast<LRUMeta *>(migration_policy_meta);

                lru_tracker.lock();
                lru_tracker.promote(lru_meta);
                lru_tracker.unlock();
        }

        bool move_row_in(ITable *table, const void *key, const std::tuple<MetaDataType *, void *> &row, bool inc_ref_cnt) override
        {
                LRUTracker &lru_tracker = lru_trackers[table->partitionID()];
                uint64_t &cur_size = migrated_row_sizes_of_partitions[table->partitionID()];
                void *migration_policy_meta = nullptr;
                LRUMeta *lru_meta = nullptr;
                bool ret = false;

                lru_tracker.lock();

                ret = move_from_partition_to_shared_region(table, key, row, inc_ref_cnt, migration_policy_meta);
                if (ret == true) {
                        CHECK(migration_policy_meta != nullptr);
                        lru_meta = reinterpret_cast<LRUMeta *>(migration_policy_meta);
                        CHECK(lru_meta->next == nullptr);
                        CHECK(lru_meta->prev == nullptr);

                        // not tracked, push it to the back
                        lru_tracker.track(lru_meta);

                        cur_size += lru_meta->row_entity.table->value_size();
                }

                lru_tracker.unlock();

                return ret;
        }

        bool move_row_out(uint64_t partition_id) override
        {
                LRUTracker &lru_tracker = lru_trackers[partition_id];
                uint64_t &cur_size = migrated_row_sizes_of_partitions[partition_id];
                bool ret = false;

                CHECK(max_migrated_rows_size_per_partition > 0);

                lru_tracker.lock();
                if (cur_size < max_migrated_rows_size_per_partition) {

                        lru_tracker.unlock();
                        return ret;
                }

                while (true) {
                        LRUMeta *victim = lru_tracker.get_next_victim();
                        if (victim == nullptr) {
                                break;
                        } else {
                                MigrationManager::migrated_row_entity victim_row_entity = victim->row_entity;
                                bool move_out_success = false;
                                move_out_success = move_from_shared_region_to_partition(victim_row_entity.table, victim_row_entity.key, victim_row_entity.local_row);
                                if (move_out_success == true) {
                                        lru_tracker.untrack(victim);
                                        lru_tracker.reset_cur_victim();
                                        cur_size -= victim_row_entity.table->value_size();
                                        if (cur_size < max_migrated_rows_size_per_partition) {
                                                ret = true;
                                                break;
                                        }
                                }
                        }
                }
                lru_tracker.reset_cur_victim();
                lru_tracker.unlock();

                return ret;
        }

        bool delete_specific_row_and_move_out(ITable *table, const void *key, bool is_delete_local) override
        {
                // key is unused
                LRUTracker &lru_tracker = lru_trackers[table->partitionID()];
                uint64_t &cur_size = migrated_row_sizes_of_partitions[table->partitionID()];
                void *migration_policy_meta = nullptr;
                LRUMeta *lru_meta = nullptr;
                bool need_move_out = false, ret = false;

                lru_tracker.lock();

                // delete and update next key information
                ret = delete_and_update_next_key_info(table, key, is_delete_local, need_move_out, migration_policy_meta);
                CHECK(ret == true);

                if (need_move_out == true) {
                        // get LRUMeta
                        CHECK(migration_policy_meta != nullptr);
                        lru_meta = reinterpret_cast<LRUMeta *>(migration_policy_meta);

                        // remove it from the LRU tracker
                        lru_tracker.untrack(lru_meta);
                        cur_size -= lru_meta->row_entity.table->value_size();
                }

                lru_tracker.unlock();

                return ret;
        }

    private:
        uint64_t max_migrated_rows_size_per_partition{ 0 };
        uint64_t *migrated_row_sizes_of_partitions{ nullptr };

        LRUTracker *lru_trackers{ nullptr };
};

} // namespace star

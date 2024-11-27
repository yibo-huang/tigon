//
// Created by Yibo Huang on 09/02/24 (Labor Day)!
//

#pragma once

#include <mutex>
#include <list>
#include "stdint.h"

#include "common/CXLMemory.h"
#include "core/Table.h"
#include "protocol/Pasha/MigrationManager.h"

namespace star
{

class PolicyClock : public MigrationManager {
    public:
        struct ClockMeta {
                uint8_t second_chance = 0;
        };

        struct ClockTrackerNode {
                ClockTrackerNode(ITable *table, const void *key, const std::tuple<MetaDataType *, void *> &row)
                        : row_entity(table, key, row, 0)
                {}

                migrated_row_entity row_entity;
                ClockTrackerNode *next{ nullptr };
                ClockTrackerNode *prev{ nullptr };
        };

        class ClockTracker {
            public:
                ClockTracker()
                : head{nullptr}
                {
                        // this spinlock will be shared between multiple processes
                        pthread_spin_init(&clock_tracker_lock, PTHREAD_PROCESS_SHARED);
                }

                void lock()
                {
                        pthread_spin_lock(&clock_tracker_lock);
                }

                void unlock()
                {
                        pthread_spin_unlock(&clock_tracker_lock);
                }

                // push back to the tail
                void track(ClockTrackerNode *node)
                {
                        if (head == nullptr && tail == nullptr) {
                                head = node;
                                tail = node;
                                node->next = nullptr;
                                node->prev = nullptr;
                        } else {
                                CHECK(head != nullptr);
                                CHECK(tail != nullptr);
                                CHECK(tail->next == nullptr);
                                CHECK(head->prev == nullptr);

                                tail->next = node;
                                node->prev = tail;
                                tail = node;
                        }
                }

                // remove from the list
                void untrack(ClockTrackerNode *node)
                {
                        if (head == nullptr && tail == nullptr) {
                                CHECK(0);
                        } else if (head == tail) {
                                CHECK(node == head);
                                CHECK(node == tail);

                                head = nullptr;
                                tail = nullptr;
                        } else {
                                if (node->prev != nullptr) {
                                        node->prev->next = node->next;
                                }
                                if (node->next != nullptr) {
                                        node->next->prev = node->prev;
                                }

                                if (head == node) {
                                        head = node->next;
                                }
                                if (tail == node) {
                                        tail = node->prev;
                                }
                        }

                        node->prev = nullptr;
                        node->next = nullptr;
                }

                // head is the victim
                ClockTrackerNode *move_forward_and_get_cursor()
                {
                        if (cursor == nullptr) {
                                cursor = head;
                        } else {
                                cursor = cursor->next;
                        }

                        return cursor;
                }

                void reset_cursor()
                {
                        cursor = nullptr;
                }

            private:
                ClockTrackerNode *head{ nullptr };
                ClockTrackerNode *tail{ nullptr };
                ClockTrackerNode *cursor{ nullptr };

                pthread_spinlock_t clock_tracker_lock;
        };

        PolicyClock(std::function<bool(ITable *, const void *, const std::tuple<std::atomic<uint64_t> *, void *> &, bool, void *&)> move_from_partition_to_shared_region,
                        std::function<bool(ITable *, const void *, const std::tuple<std::atomic<uint64_t> *, void *> &)> move_from_shared_region_to_partition,
                        std::function<bool(ITable *, const void *, bool, bool &, void *&)> delete_and_update_next_key_info,
                        uint64_t coordinator_id,
                        uint64_t partition_num,
                        const std::string when_to_move_out_str,
                        uint64_t hw_cc_budget)
        : MigrationManager(move_from_partition_to_shared_region, move_from_shared_region_to_partition, delete_and_update_next_key_info, when_to_move_out_str)
        , hw_cc_budget(hw_cc_budget)
        {
                clock_trackers = new ClockTracker[partition_num];
                for (int i = 0; i < partition_num; i++) {
                        new(&clock_trackers[i]) ClockTracker();
                }
        }

        void init_migration_policy_metadata(void *migration_policy_meta, ITable *table, const void *key, const std::tuple<MetaDataType *, void *> &row, uint64_t metadata_size) override
        {
                ClockMeta *clock_meta = reinterpret_cast<ClockMeta *>(migration_policy_meta);
                new(clock_meta) ClockMeta();
        }

        void access_row(void *migration_policy_meta, uint64_t partition_id) override
        {
                ClockMeta *clock_meta = reinterpret_cast<ClockMeta *>(migration_policy_meta);
                clock_meta->second_chance = 1;
        }

        bool move_row_in(ITable *table, const void *key, const std::tuple<MetaDataType *, void *> &row, bool inc_ref_cnt) override
        {
                ClockTracker &clock_tracker = clock_trackers[table->partitionID()];
                void *migration_policy_meta = nullptr;
                bool ret = false;

                clock_tracker.lock();
                ret = move_from_partition_to_shared_region(table, key, row, inc_ref_cnt, migration_policy_meta);
                if (ret == true) {
                        ClockTrackerNode *clock_tracker_node = new ClockTrackerNode(table, key, row);
                        clock_tracker_node->row_entity.migration_manager_meta = migration_policy_meta;
                        clock_tracker.track(clock_tracker_node);
                }
                clock_tracker.unlock();

                return ret;
        }

        bool move_row_out(uint64_t partition_id) override
        {
                ClockTracker &clock_tracker = clock_trackers[partition_id];
                bool ret = false;

                clock_tracker.lock();
                if (cxl_memory.get_stats(CXLMemory::TOTAL_HW_CC_USAGE) < hw_cc_budget) {
                        clock_tracker.unlock();
                        return ret;
                }

                while (true) {
                        ClockTrackerNode *victim = clock_tracker.move_forward_and_get_cursor();
                        if (victim == nullptr) {
                                break;
                        } else {
                                migrated_row_entity victim_row_entity = victim->row_entity;
                                ClockMeta *clock_meta = reinterpret_cast<ClockMeta *>(victim_row_entity.migration_manager_meta);
                                if (clock_meta->second_chance == 1) {
                                        clock_meta->second_chance = 0;
                                        continue;
                                }
                                bool move_out_success = false;
                                move_out_success = move_from_shared_region_to_partition(victim_row_entity.table, victim_row_entity.key, victim_row_entity.local_row);
                                if (move_out_success == true) {
                                        clock_tracker.move_forward_and_get_cursor();
                                        clock_tracker.untrack(victim);
                                        // clock_tracker.reset_cursor();
                                        if (cxl_memory.get_stats(CXLMemory::TOTAL_HW_CC_USAGE) < hw_cc_budget) {
                                                ret = true;
                                                break;
                                        }
                                }
                        }
                }
                // clock_tracker.reset_cur_victim();
                clock_tracker.unlock();

                return ret;
        }

        bool delete_specific_row_and_move_out(ITable *table, const void *key, bool is_delete_local) override
        {
                // key is unused
                ClockTracker &clock_tracker = clock_trackers[table->partitionID()];
                void *migration_policy_meta = nullptr;
                bool need_move_out = false, ret = false;

                clock_tracker.lock();

                // delete and update next key information
                ret = delete_and_update_next_key_info(table, key, is_delete_local, need_move_out, migration_policy_meta);
                CHECK(ret == true);
                CHECK(need_move_out == false);

                clock_tracker.unlock();

                return ret;
        }

    private:
        uint64_t hw_cc_budget{ 0 };

        ClockTracker *clock_trackers{ nullptr };
};

} // namespace star

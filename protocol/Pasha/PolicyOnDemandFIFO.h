//
// Created by Yibo Huang on 09/02/24 (Labor Day)!
//

#pragma once

#include <mutex>
#include <list>
#include "stdint.h"

#include "core/Table.h"
#include "protocol/Pasha/MigrationManager.h"

namespace star
{

class PolicyOnDemandFIFO : public MigrationManager {
    public:
        struct FIFOMeta {
                MigrationManager::migrated_row_entity *row_entity_ptr{ nullptr };       // this will be in local DRAM and is only accessed by the owner host
        };

        PolicyOnDemandFIFO(std::function<migration_result(ITable *, const void *, const std::tuple<std::atomic<uint64_t> *, void *> &, bool, void *&)> move_from_partition_to_shared_region,
                        std::function<bool(ITable *, const void *, const std::tuple<std::atomic<uint64_t> *, void *> &)> move_from_shared_region_to_partition,
                        std::function<bool(ITable *, const void *, bool, bool &, void *&)> delete_and_update_next_key_info,
                        const std::string when_to_move_out_str,
                        uint64_t hw_cc_budget)
        : MigrationManager(move_from_partition_to_shared_region, move_from_shared_region_to_partition, delete_and_update_next_key_info, when_to_move_out_str)
        , hw_cc_budget(hw_cc_budget)
        {}

        void init_migration_policy_metadata(void *migration_policy_meta, ITable *table, const void *key, const std::tuple<MetaDataType *, void *> &row, uint64_t metadata_size) override
        {
                FIFOMeta *fifo_meta = reinterpret_cast<FIFOMeta *>(migration_policy_meta);
                new(fifo_meta) FIFOMeta();
                CHECK(fifo_meta->row_entity_ptr == nullptr);
                fifo_meta->row_entity_ptr = new MigrationManager::migrated_row_entity(table, key, row, metadata_size);
        }

        bool move_row_in(ITable *table, const void *key, const std::tuple<MetaDataType *, void *> &row, bool inc_ref_cnt) override
        {
                void *migration_policy_meta = nullptr;
                FIFOMeta *fifo_meta = nullptr;
                migration_result ret = migration_result::FAIL_OOM;

                queue_mutex.lock();
                ret = move_from_partition_to_shared_region(table, key, row, inc_ref_cnt, migration_policy_meta);
                if (ret == migration_result::SUCCESS) {
                        CHECK(migration_policy_meta != nullptr);
                        auto fifo_meta = reinterpret_cast<FIFOMeta *>(migration_policy_meta);
                        fifo_queue.push_back(*fifo_meta->row_entity_ptr);
                }
                queue_mutex.unlock();

                return ret;
        }

        bool move_row_out(uint64_t partition_id) override
        {
                std::list<migrated_row_entity>::iterator it;
                bool ret = false;

                // move out one tuple each time
                queue_mutex.lock();
                if (cxl_memory.get_stats(CXLMemory::TOTAL_HW_CC_USAGE) < hw_cc_budget) {
                        queue_mutex.unlock();
                        return ret;
                }
                it = fifo_queue.begin();
                while (it != fifo_queue.end()) {
                        ret = move_from_shared_region_to_partition(it->table, it->key, it->local_row);
                        if (ret == true) {
                                it = fifo_queue.erase(it);
                                if (cxl_memory.get_stats(CXLMemory::TOTAL_HW_CC_USAGE) < hw_cc_budget) {
                                        break;
                                }
                        } else {
                                it++;
                        }
                }
                queue_mutex.unlock();

                return ret;
        }

        bool delete_specific_row_and_move_out(ITable *table, const void *key, bool is_delete_local) override
        {
                std::list<migrated_row_entity>::iterator it;
                void *migration_policy_meta = nullptr;
                bool need_move_out = false, ret = false;

                queue_mutex.lock();

                // delete and update next key information
                ret = delete_and_update_next_key_info(table, key, is_delete_local, need_move_out, migration_policy_meta);
                CHECK(ret == true);

                // remove it from the tracking list
                if (need_move_out == true) {
                        bool deleted = false;
                        it = fifo_queue.begin();
                        while (it != fifo_queue.end()) {
                                if (it->table->tableID() == table->tableID() && std::memcmp(it->key, key, table->key_size()) == 0) {
                                        fifo_queue.erase(it);
                                        deleted = true;
                                        break;
                                } else {
                                        it++;
                                }
                        }
                        CHECK(deleted == true);
                }

                queue_mutex.unlock();

                return ret;
        }

    private:
        uint64_t hw_cc_budget{ 0 };

        std::list<migrated_row_entity> fifo_queue;
        std::mutex queue_mutex;
};

} // namespace star

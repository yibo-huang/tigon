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
        PolicyOnDemandFIFO(std::function<bool(ITable *, const void *, const std::tuple<std::atomic<uint64_t> *, void *> &, bool, void *&)> move_from_partition_to_shared_region,
                        std::function<bool(ITable *, const void *, const std::tuple<std::atomic<uint64_t> *, void *> &)> move_from_shared_region_to_partition,
                        std::function<bool(ITable *, const void *, bool, bool &, void *&)> delete_and_update_next_key_info,
                        const std::string when_to_move_out_str,
                        uint64_t max_migrated_rows_size)
        : MigrationManager(move_from_partition_to_shared_region, move_from_shared_region_to_partition, delete_and_update_next_key_info, when_to_move_out_str)
        , max_migrated_rows_size(max_migrated_rows_size)
        {}

        bool move_row_in(ITable *table, const void *key, const std::tuple<MetaDataType *, void *> &row, bool inc_ref_cnt) override
        {
                void *migration_policy_meta = nullptr;
                bool ret = false;

                queue_mutex.lock();
                ret = move_from_partition_to_shared_region(table, key, row, inc_ref_cnt, migration_policy_meta);
                if (ret == true) {
                        fifo_queue.push_back(migrated_row_entity(table, key, row));
                        cur_size += table->value_size();
                }
                queue_mutex.unlock();

                return ret;
        }

        bool move_row_out(uint64_t partition_id) override
        {
                std::list<migrated_row_entity>::iterator it;
                bool ret = false;

                CHECK(max_migrated_rows_size > 0);

                // move out one tuple each time
                queue_mutex.lock();
                if (cur_size < max_migrated_rows_size) {
                        queue_mutex.unlock();
                        return ret;
                }
                it = fifo_queue.begin();
                while (it != fifo_queue.end()) {
                        ret = move_from_shared_region_to_partition(it->table, it->key, it->local_row);
                        if (ret == true) {
                                cur_size -= it->table->value_size();
                                it = fifo_queue.erase(it);
                                if (cur_size < max_migrated_rows_size)
                                        break;
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
        uint64_t max_migrated_rows_size;
        uint64_t cur_size{ 0 };

        std::list<migrated_row_entity> fifo_queue;
        std::mutex queue_mutex;
};

} // namespace star

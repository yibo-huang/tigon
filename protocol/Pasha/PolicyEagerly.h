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

class PolicyEagerly : public MigrationManager {
    public:
        PolicyEagerly(std::function<bool(ITable *, const void *, const std::tuple<std::atomic<uint64_t> *, void *> &, bool)> move_from_partition_to_shared_region,
                      std::function<bool(ITable *, const void *, const std::tuple<std::atomic<uint64_t> *, void *> &)> move_from_shared_region_to_partition,
                      const std::string when_to_move_out_str,
                      uint64_t max_migrated_rows_size)
        : MigrationManager(move_from_partition_to_shared_region, move_from_shared_region_to_partition, when_to_move_out_str)
        , max_migrated_rows_size(max_migrated_rows_size)
        {}

        bool move_row_in(ITable *table, const void *key, uint64_t key_size, uint64_t row_size, const std::tuple<MetaDataType *, void *> &row, bool inc_ref_cnt) override
        {
                bool ret = false;

                ret = move_from_partition_to_shared_region(table, key, row, inc_ref_cnt);
                if (ret == true) {
                        queue_mutex.lock();
                        fifo_queue.push_back(migrated_row_entity(table, key, key_size, row_size, row));
                        cur_size += row_size;
                        queue_mutex.unlock();
                }
                return ret;
        }

        bool move_row_out(uint64_t partition_id) override
        {
                std::list<migrated_row_entity>::iterator it;
                bool ret = false;

                CHECK(max_migrated_rows_size > 0);

                // eagerly moving out data
                queue_mutex.lock();
                if (cur_size < max_migrated_rows_size) {
                        queue_mutex.unlock();
                        return ret;
                }
                it = fifo_queue.begin();
                while (it != fifo_queue.end()) {
                        ret = move_from_shared_region_to_partition(it->table, it->key, it->local_row);
                        if (ret == true) {
                                cur_size -= it->row_size;
                                it = fifo_queue.erase(it);
                        } else {
                                it++;
                        }
                }
                queue_mutex.unlock();

                return true;
        }

    private:
        uint64_t max_migrated_rows_size;
        uint64_t cur_size{ 0 };

        std::list<migrated_row_entity> fifo_queue;
        std::mutex queue_mutex;
};

} // namespace star

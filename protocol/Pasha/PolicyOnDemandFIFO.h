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
        PolicyOnDemandFIFO(std::function<bool(ITable *, uint64_t, const std::tuple<std::atomic<uint64_t> *, void *> &)> move_from_partition_to_shared_region,
                        std::function<bool(ITable *, uint64_t, const std::tuple<std::atomic<uint64_t> *, void *> &)> move_from_shared_region_to_partition,
                        const std::string when_to_move_out_str,
                        uint64_t max_migrated_rows_size)
        : MigrationManager(move_from_partition_to_shared_region, move_from_shared_region_to_partition, when_to_move_out_str)
        , max_migrated_rows_size(max_migrated_rows_size)
        {}

        bool move_row_in(ITable *table, uint64_t plain_key, uint64_t size, const std::tuple<MetaDataType *, void *> &row)
        {
                bool ret = false;

                ret = move_from_partition_to_shared_region(table, plain_key, row);
                if (ret == true) {
                        queue_mutex.lock();
                        fifo_queue.push_back(migrated_row_entity(table, plain_key, size, row));
                        cur_size += size;
                        queue_mutex.unlock();
                }
                return ret;
        }

        bool move_row_out()
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
                        ret = move_from_shared_region_to_partition(it->table, it->plain_key, it->local_row);
                        if (ret == true) {
                                cur_size -= it->size;
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

    private:
        uint64_t max_migrated_rows_size;
        uint64_t cur_size{ 0 };

        std::list<migrated_row_entity> fifo_queue;
        std::mutex queue_mutex;
};

} // namespace star

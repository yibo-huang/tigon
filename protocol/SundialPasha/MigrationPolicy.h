//
// Created by Yibo Huang on 09/02/24 (Labor Day)!
//

#pragma once

#include "stdint.h"
#include <mutex>
#include <vector>

#include "core/Table.h"
#include "SundialPashaHelper.h"

namespace star
{

class MigrationManager {
    public:
        using MetaDataType = std::atomic<uint64_t>;
        class row_track_entity
        {
            public:
                row_track_entity() = default;
                row_track_entity(ITable *table, uint64_t plain_key, const std::tuple<MetaDataType *, void *> row)
                        : table(table)
                        , plain_key(plain_key)
                        , local_row(row)
                {}

                ITable *table;
                uint64_t plain_key;
                std::tuple<MetaDataType *, void *> local_row;
        };

        bool move_single_row_in(ITable *table, uint64_t plain_key, const std::tuple<MetaDataType *, void *> &row)
        {
                bool ret = false;

                move_single_row_out();

                ret = global_helper.move_from_partition_to_shared_region(table, plain_key, row);
                if (ret == true) {
                        queue_mutex.lock();
                        fifo_queue.push_back(row_track_entity(table, plain_key, row));
                        queue_mutex.unlock();
                }
                return ret;
        }

        bool move_single_row_out()
        {
                row_track_entity entity_to_evict;
                std::vector<row_track_entity>::iterator it;
                bool ret = false;

                queue_mutex.lock();
                if (fifo_queue.size() > max_row_num) {
                        for (it = fifo_queue.begin(); it != fifo_queue.end(); it++) {
                                entity_to_evict = fifo_queue.front();
                                ret = global_helper.move_from_shared_region_to_partition(it->table, it->plain_key, it->local_row);
                                if (ret == true)
                                        break;
                        }
                        if (ret == true) {
                                // LOG(INFO) << "moved out row with key " << it->plain_key << " from table " << it->table->tableID();
                                fifo_queue.erase(it);
                        }
                }
                queue_mutex.unlock();

                return ret;
        }

    private:
        static constexpr std::size_t max_row_num = 10000;
        std::vector<row_track_entity> fifo_queue;
        std::mutex queue_mutex;
};

extern MigrationManager migration_manager;

} // namespace star

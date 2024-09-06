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
        PolicyEagerly(std::function<bool(ITable *, uint64_t, const std::tuple<std::atomic<uint64_t> *, void *> &)> move_from_partition_to_shared_region,
                         std::function<bool(ITable *, uint64_t, const std::tuple<std::atomic<uint64_t> *, void *> &)> move_from_shared_region_to_partition)
        : MigrationManager(move_from_partition_to_shared_region, move_from_shared_region_to_partition)
        {}

        bool move_row_in(ITable *table, uint64_t plain_key, const std::tuple<MetaDataType *, void *> &row)
        {
                bool ret = false;

                queue_mutex.lock();
                ret = move_from_partition_to_shared_region(table, plain_key, row);
                if (ret == true) {
                        fifo_queue.push_back(migrated_row_entity(table, plain_key, row));
                }
                queue_mutex.unlock();
                return ret;
        }

        bool move_row_out()
        {
                std::list<migrated_row_entity>::iterator it;
                bool ret = false;

                // eagerly moving out data
                queue_mutex.lock();
                it = fifo_queue.begin();
                while (it != fifo_queue.end()) {
                        ret = move_from_shared_region_to_partition(it->table, it->plain_key, it->local_row);
                        if (ret == true) {
                                it = fifo_queue.erase(it);
                        } else {
                                it++;
                        }
                }
                queue_mutex.unlock();

                return true;
        }

    private:
        std::list<migrated_row_entity> fifo_queue;
        std::mutex queue_mutex;
};

} // namespace star

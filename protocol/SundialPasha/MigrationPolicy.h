//
// Created by Yibo Huang on 09/02/24 (Labor Day)!
//

#pragma once

#include "stdint.h"
#include <mutex>
#include <queue>

namespace star
{

class MigrationPolicy {
    public:
        void put_row_to_track(void *row)
        {
                queue_mutex.lock();
                fifo_queue.push(row);
                queue_mutex.unlock();
        }

        void *get_row_to_evict()
        {
                void *row_to_evict = nullptr;

                queue_mutex.lock();
                row_to_evict = fifo_queue.front();
                if (row_to_evict != nullptr)
                        fifo_queue.pop();
                queue_mutex.unlock();

                return row_to_evict;
        }

    private:
        std::queue<void *> fifo_queue;
        std::mutex queue_mutex;
};

} // namespace star

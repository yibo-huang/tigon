//
// Created by Yibo Huang on 8/8/24.
//

#pragma once

#include "common/Message.h"
#include "common/CXLMemory.h"
#include <boost/interprocess/offset_ptr.hpp>
#include <stddef.h>
#include <atomic>
#include <glog/logging.h>

namespace star
{

class MPSCRingBuffer {
    public:
        static constexpr uint64_t entry_struct_size = 8192;
        static constexpr uint64_t entry_data_size = entry_struct_size - 9;

        struct Entry {
                uint32_t remaining_size;
                uint32_t dequeue_offset;
                std::atomic<uint8_t> is_ready;
                uint8_t data[entry_data_size];
        };

        MPSCRingBuffer(uint64_t entry_num)
                : entry_num(entry_num)
                , head(0)
                , tail(0)
                , count(0)
        {
                int i = 0;

                DCHECK(entry_struct_size == sizeof(Entry));
                entries = reinterpret_cast<Entry *>(cxl_memory.cxlalloc_malloc_wrapper(sizeof(Entry) * entry_num,
                        CXLMemory::TRANSPORT_ALLOCATION));
                for (i = 0; i < entry_num; i++) {
                        entries[i].is_ready = 0;
                        entries[i].remaining_size = 0;
                        entries[i].dequeue_offset = 0;
                        memset(entries[i].data, 0, entry_data_size);
                }
        }

        uint64_t get_entry_num()
        {
                return entry_num;
        }

        uint64_t get_entry_size()
        {
                return entry_data_size;
        }

        uint64_t size()
        {
                uint64_t cur_head = 0, cur_tail = 0;
                uint64_t ret = 0;

                cur_head = head.load(std::memory_order_acquire);
                cur_tail = (tail.load(std::memory_order_acquire)) % entry_num;
                ret = (cur_tail - cur_head + entry_num) % entry_num;

                return ret;
        }

        bool enqueue(char *data, uint64_t data_size)
        {
                uint64_t cur_count = 0, cur_tail = 0;

                DCHECK(data_size <= entry_data_size);

                /* try to gain access to the queue */
                cur_count = std::atomic_fetch_add_explicit(&count, 1, std::memory_order_acquire);
                if(cur_count >= entry_num) {
                        /* back off since queue is full */
                        std::atomic_fetch_sub_explicit(&count, 1, std::memory_order_release);
                        return false;
                }

                /* gain exclusive access to the entry */
                cur_tail = std::atomic_fetch_add_explicit(&tail, 1, std::memory_order_release);
                cur_tail %= entry_num;

                /* memcpy the data to the target endpoint's receive queue */
                memcpy(entries[cur_tail].data, data, data_size);
                entries[cur_tail].remaining_size = data_size;
                entries[cur_tail].dequeue_offset = 0;

                /* mark the entry as ready */
                entries[cur_tail].is_ready.store(1, std::memory_order_release);

                return true;
        }

        uint64_t dequeue(char *data_buffer, uint64_t buffer_size)
        {
                uint64_t cur_head = 0, cur_tail = 0;
                uint64_t dequeue_size = 0;

                if (buffer_size == 0)
                        return 0;

                if (size() == 0)
                        return 0;

                cur_head = head.load(std::memory_order_acquire);

                /* wait for the entry to be ready */
                while (entries[cur_head].is_ready.load(std::memory_order_acquire) != 1);

                /* only dequeue part of the data if the buffer is not large enough */
                if (buffer_size < entries[cur_head].remaining_size)
                        dequeue_size = buffer_size;
                else
                        dequeue_size = entries[cur_head].remaining_size;

                /* partial dequeue is not supported for now */
                DCHECK(dequeue_size == entries[cur_head].remaining_size);

                /* memcpy the data to the user-provided buffer and update the metadata */
                memcpy(data_buffer, entries[cur_head].data, dequeue_size);
                entries[cur_head].dequeue_offset += dequeue_size;
                entries[cur_head].remaining_size -= dequeue_size;

                if (entries[cur_head].remaining_size == 0) {
                        /* reset metadata */
                        entries[cur_tail].dequeue_offset = 0;

                        /* mark it as not ready */
                        entries[cur_head].is_ready.store(0, std::memory_order_relaxed);

                        /* increase head by 1 */
                        head.store((cur_head + 1) % entry_num, std::memory_order_release);

                        /* reduce count by 1 */
                        std::atomic_fetch_sub_explicit(&count, 1, std::memory_order_release);
                }

                return dequeue_size;
        }

        uint64_t send(char *data, uint64_t data_size)
        {
                while (enqueue(data, data_size) != true);
                return data_size;
        }

        uint64_t recv(char *buffer, uint64_t buffer_size)
        {
                uint64_t available_entries = size();
                uint64_t data_size = 0;
                int i = 0;

                if (available_entries == 0)
                        return 0;

                data_size = dequeue(buffer, buffer_size);
                return data_size;
        }

    private:
        uint64_t entry_num;

        std::atomic<uint64_t> head;
        std::atomic<uint64_t> tail;
        std::atomic<uint64_t> count;
        boost::interprocess::offset_ptr<Entry> entries;
};

}

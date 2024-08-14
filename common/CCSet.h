//
// Crash-Consistent Set (not thread-safe)
// Created by Yibo Huang on 8/13/24.
//

#pragma once

#include <atomic>
#include "stdint.h"
#include <boost/interprocess/offset_ptr.hpp>

namespace star
{

class CCSet {
    public:
        static constexpr uint64_t max_capacity = 100;

        uint64_t size()
        {
                return cur_size.load(std::memory_order_acquire);
        }

        bool empty(void)
        {
                return cur_size.load(std::memory_order_acquire) == 0;
        }

        void clear(void)
        {
                cur_size.store(0, std::memory_order_release);
        }

        void insert(char *row)
        {
                uint64_t size = cur_size.load(std::memory_order_acquire);
                int i = 0;

                /* check for duplicates */
                for (i = 0; i < size; i++)
                        if (rows[i] == row)
                                return;

                /* insert the row */
                assert(size != max_capacity);
                rows[size] = row;
                cur_size.store(size + 1, std::memory_order_release);     // linearization point
        }

        void remove(char *row)
        {
                /* not implemented */
	        assert(0);
        }

    private:
        boost::interprocess::offset_ptr<char> rows[max_capacity];
        std::atomic<uint64_t> cur_size;
};

} // namespace star

//
// Crash-Consistent Set (not thread-safe)
// Created by Yibo Huang on 8/13/24.
//

#pragma once

#include "stdint.h"
#include <glog/logging.h>
#include <boost/interprocess/offset_ptr.hpp>

namespace star
{

// TODO: make it crash-consistent
class CCSet {
    public:
        static constexpr uint64_t max_capacity = 5;

        CCSet()
		: cur_size(0)
	{
                for (int i = 0; i < max_capacity; i++)
                        rows[i] = nullptr;
	}

        char *get_element(uint64_t index)
        {
                return rows[index].get();
        }

        uint64_t size()
        {
                return cur_size;
        }

        bool empty()
        {
                return cur_size == 0;
        }

        void clear()
        {
                cur_size = 0;
                for (int i = 0; i < max_capacity; i++)
                        rows[i] = nullptr;
        }

        bool insert(char *row)
        {
                /* check for duplicates */
                for (int i = 0; i < cur_size; i++)
                        if (rows[i].get() == row)
                                return false;

                /* insert the row */
                CHECK(cur_size != max_capacity);
                rows[cur_size] = row;
                cur_size++;
                return true;
        }

        bool remove(char *row)
        {
                int index = -1;
                bool ret = false;

                /* find the row */
                for (int i = 0; i < cur_size; i++) {
                        if (rows[i].get() == row) {
                                index = i;
                                break;
                        }
                }

                /* remove the row */
                if (index != -1) {
                        rows[index] = nullptr;
                        for (int i = index; i < cur_size - 1; i++) {
                                rows[i] = rows[i + 1];
                        }
                        rows[cur_size - 1] = nullptr;
                        cur_size--;
                        ret = true;
                }
                return ret;
        }

    private:
        boost::interprocess::offset_ptr<char> rows[max_capacity];
        uint64_t cur_size;
};

} // namespace star

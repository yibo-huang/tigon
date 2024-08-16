//
// Crash-Consistent Set (not thread-safe)
// Created by Yibo Huang on 8/13/24.
//

#pragma once

#include "stdint.h"
#include <boost/interprocess/offset_ptr.hpp>

namespace star
{

class CCSet {
    public:
        static constexpr uint64_t max_capacity = 100;

        CCSet()
		: cur_size(0)
	{
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
        }

        void insert(char *row)
        {
                int i = 0;

                /* check for duplicates */
                for (i = 0; i < cur_size; i++)
                        if (rows[i] == row)
                                return;

                /* insert the row */
                assert(cur_size != max_capacity);
                rows[cur_size] = row;
                cur_size++;
        }

        void remove(char *row)
        {
                /* not implemented */
	        assert(0);
        }

    private:
        boost::interprocess::offset_ptr<char> rows[max_capacity];
        uint64_t cur_size;
};

} // namespace star

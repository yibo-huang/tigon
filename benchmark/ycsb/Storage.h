//
// Created by Yi Lu on 9/12/18.
//

#pragma once

#include "core/Table.h"
#include "benchmark/ycsb/Schema.h"

namespace star
{

namespace ycsb
{
struct Storage {
	ycsb::key ycsb_keys[YCSB_FIELD_SIZE];
	ycsb::value ycsb_values[YCSB_FIELD_SIZE];

        ycsb::key ycsb_scan_min_keys[YCSB_FIELD_SIZE];
        ycsb::key ycsb_scan_max_keys[YCSB_FIELD_SIZE];
        std::vector<ITable::row_entity> ycsb_scan_results[YCSB_FIELD_SIZE];

        void cleanup()
        {
                for (int i = 0; i < YCSB_FIELD_SIZE; i++) {
                        ycsb_scan_results[i].clear();
                }
        }
};

} // namespace ycsb
} // namespace star
//
// Created by Yi Lu on 9/12/18.
//

#pragma once

#include "core/Table.h"
#include "benchmark/tatp/Schema.h"

namespace star
{

namespace tatp
{
struct Storage {
	subscriber::key subscriber_key;
	subscriber::value subscriber_value;

        access_info::key access_info_key;
	access_info::value access_info_value;

        void cleanup()
        {
                // do nothing
        }
};

} // namespace tatp
} // namespace star

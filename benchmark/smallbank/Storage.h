//
// Created by Yi Lu on 9/12/18.
//

#pragma once

#include "core/Table.h"
#include "benchmark/smallbank/Schema.h"

namespace star
{

namespace smallbank
{
struct Storage {
	savings::key savings_key;
	savings::value savings_value;

        checking::key first_checking_key;
	checking::value first_checking_value;

        checking::key second_checking_key;
	checking::value second_checking_value;

        void cleanup()
        {
                // do nothing
        }
};

} // namespace smallbank
} // namespace star
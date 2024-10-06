//
// Created by Yi Lu on 9/12/18.
//

#pragma once

#include "benchmark/tpcc/Schema.h"

namespace star
{
namespace tpcc
{
struct Storage {
	warehouse::key warehouse_key;
	warehouse::value warehouse_value;

	district::key district_key;
	district::value district_value;

	customer_name_idx::key customer_name_idx_key;
	customer_name_idx::value customer_name_idx_value;

	customer::key customer_key[DISTRICT_PER_WAREHOUSE];
	customer::value customer_value[DISTRICT_PER_WAREHOUSE];

	item::key item_keys[15];
	item::value item_values[15];

	stock::key stock_keys[MAX_ORDER_LINE_PER_ORDER * 20];
	stock::value stock_values[MAX_ORDER_LINE_PER_ORDER * 20];

        new_order::key oldest_undelivered_order_key;
        new_order::value oldest_undelivered_order_value;        // never used though
	new_order::key new_order_key;
        new_order::value new_order_value;
        new_order::key min_new_order_key[DISTRICT_PER_WAREHOUSE];
        new_order::key max_new_order_key[DISTRICT_PER_WAREHOUSE];
        std::vector<std::tuple<new_order::key, std::atomic<uint64_t> *, void *> > new_order_scan_results[DISTRICT_PER_WAREHOUSE];

	order::key order_key[DISTRICT_PER_WAREHOUSE];
	order::value order_value[DISTRICT_PER_WAREHOUSE];

        order_customer::key min_order_customer_key;
        order_customer::key max_order_customer_key;
        std::vector<std::tuple<order_customer::key, std::atomic<uint64_t> *, void *> > order_customer_scan_results;

        order_line::key order_line_keys[DISTRICT_PER_WAREHOUSE][15];
	order_line::value order_line_values[DISTRICT_PER_WAREHOUSE][15];
        order_line::key min_order_line_key[DISTRICT_PER_WAREHOUSE];
        order_line::key max_order_line_key[DISTRICT_PER_WAREHOUSE];
        std::vector<std::tuple<order_line::key, std::atomic<uint64_t> *, void *> > order_line_scan_results[DISTRICT_PER_WAREHOUSE];

	history::key history_key;
	history::value history_value;

        void cleanup()
        {
                order_customer_scan_results.clear();
                for (int i = 0; i < DISTRICT_PER_WAREHOUSE; i++) {
                        new_order_scan_results[i].clear();
                        order_line_scan_results[i].clear();
                }
        }
};
} // namespace tpcc
} // namespace star
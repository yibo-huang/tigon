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

class PolicyNoMoveOut : public MigrationManager {
    public:
        PolicyNoMoveOut(std::function<bool(ITable *, uint64_t, const std::tuple<std::atomic<uint64_t> *, void *> &)> move_from_partition_to_shared_region,
                        std::function<bool(ITable *, uint64_t, const std::tuple<std::atomic<uint64_t> *, void *> &)> move_from_shared_region_to_partition,
                        const std::string when_to_move_out_str)
        : MigrationManager(move_from_partition_to_shared_region, move_from_shared_region_to_partition, when_to_move_out_str)
        {}

        bool move_row_in(ITable *table, uint64_t plain_key, uint64_t size, const std::tuple<MetaDataType *, void *> &row)
        {
                return move_from_partition_to_shared_region(table, plain_key, row);
        }

        bool move_row_out()
        {
                return true;
        }
};

} // namespace star

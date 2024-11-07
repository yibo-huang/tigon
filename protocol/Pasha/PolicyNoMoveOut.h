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
        PolicyNoMoveOut(std::function<bool(ITable *, const void *, const std::tuple<std::atomic<uint64_t> *, void *> &, bool)> move_from_partition_to_shared_region,
                        std::function<bool(ITable *, const void *, const std::tuple<std::atomic<uint64_t> *, void *> &)> move_from_shared_region_to_partition,
                        const std::string when_to_move_out_str)
        : MigrationManager(move_from_partition_to_shared_region, move_from_shared_region_to_partition, when_to_move_out_str)
        {}

        bool move_row_in(ITable *table, const void *key, const std::tuple<MetaDataType *, void *> &row, bool inc_ref_cnt) override
        {
                return move_from_partition_to_shared_region(table, key, row, inc_ref_cnt);
        }

        bool move_row_out(uint64_t partition_id) override
        {
                return true;
        }

        bool move_row_out_without_copyback(ITable *table, const void *key, void *migration_policy_meta) override
        {
                return twopl_pasha_global_helper->remove_migrated_row(table->tableID(), table->partitionID(), key);
        }
};

} // namespace star

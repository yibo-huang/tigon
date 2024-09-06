//
// Created by Yibo Huang on 09/02/24 (Labor Day)!
//

#pragma once

#include "stdint.h"
#include "core/Table.h"

namespace star
{

class MigrationManager {
    public:
        using MetaDataType = std::atomic<uint64_t>;

        MigrationManager(std::function<bool(ITable *, uint64_t, const std::tuple<std::atomic<uint64_t> *, void *> &)> move_from_partition_to_shared_region,
                         std::function<bool(ITable *, uint64_t, const std::tuple<std::atomic<uint64_t> *, void *> &)> move_from_shared_region_to_partition)
        : move_from_partition_to_shared_region(move_from_partition_to_shared_region)
        , move_from_shared_region_to_partition(move_from_shared_region_to_partition)
        {}

        class migrated_row_entity
        {
            public:
                migrated_row_entity() = default;
                migrated_row_entity(ITable *table, uint64_t plain_key, const std::tuple<MetaDataType *, void *> row)
                        : table(table)
                        , plain_key(plain_key)
                        , local_row(row)
                {}

                ITable *table;
                uint64_t plain_key;
                std::tuple<MetaDataType *, void *> local_row;
        };

        virtual bool move_row_in(ITable *table, uint64_t plain_key, const std::tuple<MetaDataType *, void *> &row) = 0;
        virtual bool move_row_out() = 0;

        // user-provided functions
        std::function<bool(ITable *, uint64_t, const std::tuple<std::atomic<uint64_t> *, void *> &)> move_from_partition_to_shared_region;
        std::function<bool(ITable *, uint64_t, const std::tuple<std::atomic<uint64_t> *, void *> &)> move_from_shared_region_to_partition;
};

extern MigrationManager *migration_manager;

} // namespace star

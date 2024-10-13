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

        enum {
                OnDemand,
                Reactive
        };

        MigrationManager(std::function<bool(ITable *, const void *, const std::tuple<std::atomic<uint64_t> *, void *> &)> move_from_partition_to_shared_region,
                         std::function<bool(ITable *, const void *, const std::tuple<std::atomic<uint64_t> *, void *> &)> move_from_shared_region_to_partition,
                         const std::string when_to_move_out_str)
        : move_from_partition_to_shared_region(move_from_partition_to_shared_region)
        , move_from_shared_region_to_partition(move_from_shared_region_to_partition)
        {
                if (when_to_move_out_str == "OnDemand") {
                        when_to_move_out = OnDemand;
                } else if (when_to_move_out_str == "Reactive") {
                        when_to_move_out = Reactive;
                } else {
                        CHECK(0);
                }
        }

        class migrated_row_entity
        {
            public:
                migrated_row_entity() = default;
                migrated_row_entity(ITable *table, const void *key, uint64_t key_size, uint64_t row_size, const std::tuple<MetaDataType *, void *> row)
                        : table(table)
                        , key_size(key_size)
                        , row_size(row_size)
                        , local_row(row)
                {
                        memcpy(this->key, key, key_size);
                }

                static constexpr uint64_t max_key_size = 64;
                ITable *table;
                char key[max_key_size];
                uint64_t key_size;
                uint64_t row_size;
                std::tuple<MetaDataType *, void *> local_row;
        };

        virtual bool move_row_in(ITable *table, const void *key, uint64_t key_size, uint64_t row_size, const std::tuple<MetaDataType *, void *> &row) = 0;
        virtual bool move_row_out() = 0;

        // user-provided functions
        std::function<bool(ITable *, const void *, const std::tuple<std::atomic<uint64_t> *, void *> &)> move_from_partition_to_shared_region;
        std::function<bool(ITable *, const void *, const std::tuple<std::atomic<uint64_t> *, void *> &)> move_from_shared_region_to_partition;

        // when to move out
        int when_to_move_out;

        std::atomic<uint64_t> n_data_move_in{ 0 }, n_data_move_out{ 0 };
};

extern MigrationManager *migration_manager;

extern std::atomic<uint64_t> num_data_move_in;
extern std::atomic<uint64_t> num_data_move_out;

} // namespace star

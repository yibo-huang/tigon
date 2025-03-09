//
// Created by Yibo Huang on 09/02/24 (Labor Day)!
//

#pragma once

#include "stdint.h"
#include "core/Table.h"

namespace star
{

enum migration_result {
        SUCCESS,
        FAIL_ALREADY_IN_CXL,
        FAIL_OOM
};

class MigrationManager {
    public:
        using MetaDataType = std::atomic<uint64_t>;

        static constexpr uint64_t migration_policy_meta_size = 24;        // in bytes

        enum {
                OnDemand,
                Reactive
        };

        MigrationManager(std::function<migration_result(ITable *, const void *, const std::tuple<std::atomic<uint64_t> *, void *> &, bool, void *&)> move_from_partition_to_shared_region,
                         std::function<bool(ITable *, const void *, const std::tuple<std::atomic<uint64_t> *, void *> &)> move_from_shared_region_to_partition,
                         std::function<bool(ITable *, const void *, bool, bool &, void *&)> delete_and_update_next_key_info,
                         const std::string when_to_move_out_str)
        : move_from_partition_to_shared_region(move_from_partition_to_shared_region)
        , move_from_shared_region_to_partition(move_from_shared_region_to_partition)
        , delete_and_update_next_key_info(delete_and_update_next_key_info)
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
                migrated_row_entity(ITable *table, const void *key, const std::tuple<MetaDataType *, void *> row, uint64_t metadata_size)
                        : table(table)
                        , metadata_size(metadata_size)
                        , local_row(row)
                {
                        memcpy(this->key, key, table->key_size());
                }

                static constexpr uint64_t max_key_size = 64;
                ITable *table{ nullptr };
                char key[max_key_size];
                uint64_t metadata_size{ 0 };
                std::tuple<MetaDataType *, void *> local_row;
                void *migration_manager_meta{ nullptr };
        };

        virtual void init_migration_policy_metadata(void *migration_policy_meta, ITable *table, const void *key, const std::tuple<MetaDataType *, void *> &row, uint64_t metadata_size)
        {
        }

        virtual void access_row(void *migration_policy_meta, uint64_t partition_id)
        {
        }

        virtual bool move_row_in(ITable *table, const void *key, const std::tuple<MetaDataType *, void *> &row, bool inc_ref_cnt) = 0;
        virtual bool move_row_out(uint64_t partition_id) = 0;
        virtual bool delete_specific_row_and_move_out(ITable *table, const void *key, bool is_delete_local) = 0;

        // user-provided functions
        std::function<migration_result(ITable *, const void *, const std::tuple<std::atomic<uint64_t> *, void *> &, bool inc_ref_cnt, void *&)> move_from_partition_to_shared_region;
        std::function<bool(ITable *, const void *, const std::tuple<std::atomic<uint64_t> *, void *> &)> move_from_shared_region_to_partition;
        std::function<bool(ITable *, const void *, bool, bool &, void *&)> delete_and_update_next_key_info;

        // when to move out
        int when_to_move_out;

        std::atomic<uint64_t> n_data_move_in{ 0 }, n_data_move_out{ 0 };
};

extern MigrationManager *migration_manager;

extern std::atomic<uint64_t> num_data_move_in;
extern std::atomic<uint64_t> num_data_move_out;

} // namespace star

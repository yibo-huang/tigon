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
};

extern MigrationManager *migration_manager;

} // namespace star

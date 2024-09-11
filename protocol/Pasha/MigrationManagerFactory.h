//
// Created by Yi Lu on 9/7/18.
//

#pragma once

#include <string>

#include "protocol/Pasha/MigrationManager.h"
#include "protocol/Pasha/PolicyEagerly.h"
#include "protocol/Pasha/PolicyOnDemandFIFO.h"
#include "protocol/Pasha/PolicyNoMoveOut.h"

#include "protocol/SundialPasha/SundialPashaHelper.h"

namespace star
{

class MigrationManagerFactory {
    public:
	static MigrationManager *create_migration_manager(const std::string &protocol, const std::string &migration_policy,
                                                          const std::string &when_to_move_out, uint64_t max_migrated_rows_size)
	{
                MigrationManager *migration_manager = nullptr;

                if (protocol == "SundialPasha") {
                        if (migration_policy == "OnDemandFIFO") {
                                migration_manager = new PolicyOnDemandFIFO(
                                        std::bind(&SundialPashaHelper::move_from_partition_to_shared_region, &global_helper, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3),
                                        std::bind(&SundialPashaHelper::move_from_shared_region_to_partition, &global_helper, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3),
                                        when_to_move_out,
                                        max_migrated_rows_size);
                        } else if (migration_policy == "Eagerly") {
                                migration_manager = new PolicyEagerly(
                                        std::bind(&SundialPashaHelper::move_from_partition_to_shared_region, &global_helper, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3),
                                        std::bind(&SundialPashaHelper::move_from_shared_region_to_partition, &global_helper, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3),
                                        when_to_move_out,
                                        max_migrated_rows_size);
                        } else if (migration_policy == "NoMoveOut") {
                                migration_manager = new PolicyNoMoveOut(
                                        std::bind(&SundialPashaHelper::move_from_partition_to_shared_region, &global_helper, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3),
                                        std::bind(&SundialPashaHelper::move_from_shared_region_to_partition, &global_helper, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3),
                                        when_to_move_out);
                        } else {
                                CHECK(0);
                        }
                } else {
                        CHECK(0);
                }

		return migration_manager;
	}
};

} // namespace star

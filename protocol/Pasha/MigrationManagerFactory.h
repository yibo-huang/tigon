//
// Created by Yi Lu on 9/7/18.
//

#pragma once

#include <string>

#include "protocol/Pasha/MigrationManager.h"
#include "protocol/Pasha/PolicyEagerly.h"
#include "protocol/Pasha/PolicyOnDemandFIFO.h"
#include "protocol/Pasha/PolicyNoMoveOut.h"
#include "protocol/Pasha/PolicyLRU.h"

#include "protocol/SundialPasha/SundialPashaHelper.h"

namespace star
{

class MigrationManagerFactory {
    public:
	static MigrationManager *create_migration_manager(const std::string &protocol, const std::string &migration_policy, uint64_t coordinator_id,
                                                        uint64_t partition_num, const std::string &when_to_move_out, uint64_t hw_cc_budget)
	{
                MigrationManager *migration_manager = nullptr;

                if (protocol == "SundialPasha") {
                        if (migration_policy == "OnDemandFIFO") {
                                migration_manager = new PolicyOnDemandFIFO(
                                        std::bind(&SundialPashaHelper::move_from_partition_to_shared_region, sundial_pasha_global_helper, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3, std::placeholders::_4, std::placeholders::_5),
                                        std::bind(&SundialPashaHelper::move_from_shared_region_to_partition, sundial_pasha_global_helper, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3),
                                        std::bind(&SundialPashaHelper::delete_and_update_next_key_info, sundial_pasha_global_helper, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3, std::placeholders::_4, std::placeholders::_5),
                                        when_to_move_out,
                                        hw_cc_budget);
                        } else if (migration_policy == "Eagerly") {
                                migration_manager = new PolicyEagerly(
                                        std::bind(&SundialPashaHelper::move_from_partition_to_shared_region, sundial_pasha_global_helper, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3, std::placeholders::_4, std::placeholders::_5),
                                        std::bind(&SundialPashaHelper::move_from_shared_region_to_partition, sundial_pasha_global_helper, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3),
                                        std::bind(&SundialPashaHelper::delete_and_update_next_key_info, sundial_pasha_global_helper, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3, std::placeholders::_4, std::placeholders::_5),
                                        when_to_move_out);
                        } else if (migration_policy == "NoMoveOut") {
                                migration_manager = new PolicyNoMoveOut(
                                        std::bind(&SundialPashaHelper::move_from_partition_to_shared_region, sundial_pasha_global_helper, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3, std::placeholders::_4, std::placeholders::_5),
                                        std::bind(&SundialPashaHelper::move_from_shared_region_to_partition, sundial_pasha_global_helper, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3),
                                        std::bind(&SundialPashaHelper::delete_and_update_next_key_info, sundial_pasha_global_helper, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3, std::placeholders::_4, std::placeholders::_5),
                                        when_to_move_out);
                        } else if (migration_policy == "LRU") {
                                migration_manager = new PolicyLRU(
                                        std::bind(&SundialPashaHelper::move_from_partition_to_shared_region, sundial_pasha_global_helper, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3, std::placeholders::_4, std::placeholders::_5),
                                        std::bind(&SundialPashaHelper::move_from_shared_region_to_partition, sundial_pasha_global_helper, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3),
                                        std::bind(&SundialPashaHelper::delete_and_update_next_key_info, sundial_pasha_global_helper, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3, std::placeholders::_4, std::placeholders::_5),
                                        coordinator_id,
                                        partition_num,
                                        when_to_move_out,
                                        hw_cc_budget);
                        } else {
                                CHECK(0);
                        }
                } else if (protocol == "TwoPLPasha") {
                        if (migration_policy == "OnDemandFIFO") {
                                migration_manager = new PolicyOnDemandFIFO(
                                        std::bind(&TwoPLPashaHelper::move_from_partition_to_shared_region, twopl_pasha_global_helper, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3, std::placeholders::_4, std::placeholders::_5),
                                        std::bind(&TwoPLPashaHelper::move_from_shared_region_to_partition, twopl_pasha_global_helper, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3),
                                        std::bind(&TwoPLPashaHelper::delete_and_update_next_key_info, twopl_pasha_global_helper, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3, std::placeholders::_4, std::placeholders::_5),
                                        when_to_move_out,
                                        hw_cc_budget);
                        } else if (migration_policy == "Eagerly") {
                                migration_manager = new PolicyEagerly(
                                        std::bind(&TwoPLPashaHelper::move_from_partition_to_shared_region, twopl_pasha_global_helper, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3, std::placeholders::_4, std::placeholders::_5),
                                        std::bind(&TwoPLPashaHelper::move_from_shared_region_to_partition, twopl_pasha_global_helper, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3),
                                        std::bind(&TwoPLPashaHelper::delete_and_update_next_key_info, twopl_pasha_global_helper, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3, std::placeholders::_4, std::placeholders::_5),
                                        when_to_move_out);
                        } else if (migration_policy == "NoMoveOut") {
                                migration_manager = new PolicyNoMoveOut(
                                        std::bind(&TwoPLPashaHelper::move_from_partition_to_shared_region, twopl_pasha_global_helper, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3, std::placeholders::_4, std::placeholders::_5),
                                        std::bind(&TwoPLPashaHelper::move_from_shared_region_to_partition, twopl_pasha_global_helper, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3),
                                        std::bind(&TwoPLPashaHelper::delete_and_update_next_key_info, twopl_pasha_global_helper, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3, std::placeholders::_4, std::placeholders::_5),
                                        when_to_move_out);
                        } else if (migration_policy == "LRU") {
                                migration_manager = new PolicyLRU(
                                        std::bind(&TwoPLPashaHelper::move_from_partition_to_shared_region, twopl_pasha_global_helper, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3, std::placeholders::_4, std::placeholders::_5),
                                        std::bind(&TwoPLPashaHelper::move_from_shared_region_to_partition, twopl_pasha_global_helper, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3),
                                        std::bind(&TwoPLPashaHelper::delete_and_update_next_key_info, twopl_pasha_global_helper, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3, std::placeholders::_4, std::placeholders::_5),
                                        coordinator_id,
                                        partition_num,
                                        when_to_move_out,
                                        hw_cc_budget);
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

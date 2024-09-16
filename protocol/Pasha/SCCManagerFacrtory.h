//
// Created by Yi Lu on 9/7/18.
//

#pragma once

#include <string>

#include "protocol/Pasha/SCCManager.h"
#include "protocol/Pasha/SCCAllNTP.h"
#include "protocol/Pasha/SCCNoOP.h"

namespace star
{

class SCCManagerFactory {
    public:
	static SCCManager *create_scc_manager(const std::string &protocol, const std::string &scc_mechanism)
	{
                SCCManager *scc_manager = nullptr;

                if (protocol == "SundialPasha") {
                        if (scc_mechanism == "AllNTP") {
                                scc_manager = new SCCAllNTP();
                        } else if (scc_mechanism == "NoOP") {
                                LOG(INFO) << "reach here";
                                scc_manager = new SCCNoOP();
                        } else {
                                CHECK(0);
                        }
                } else {
                        CHECK(0);
                }

		return scc_manager;
	}
};

} // namespace star

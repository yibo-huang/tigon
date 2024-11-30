//
// Created by Yi Lu on 9/7/18.
//

#pragma once

#include <string>

#include "protocol/Pasha/SCCManager.h"
#include "protocol/Pasha/SCCNonTemporal.h"
#include "protocol/Pasha/SCCNoOP.h"
#include "protocol/Pasha/SCCWriteThrough.h"
#include "protocol/TwoPLPasha/TwoPLPashaSCCWriteThrough.h"

namespace star
{

class SCCManagerFactory {
    public:
	static SCCManager *create_scc_manager(const std::string &protocol, const std::string &scc_mechanism)
	{
                SCCManager *scc_manager = nullptr;

                if (protocol == "SundialPasha") {
                        if (scc_mechanism == "NonTemporal") {
                                scc_manager = new SCCNonTemporal();
                        } else if (scc_mechanism == "NoOP") {
                                scc_manager = new SCCNoOP();
                        } else if (scc_mechanism == "WriteThrough") {
                                scc_manager = new SCCWriteThrough();
                        } else {
                                CHECK(0);
                        }
                } else if (protocol == "TwoPLPasha") {
                        if (scc_mechanism == "NonTemporal") {
                                scc_manager = new SCCNonTemporal();
                        } else if (scc_mechanism == "NoOP") {
                                scc_manager = new SCCNoOP();
                        } else if (scc_mechanism == "WriteThrough") {
                                scc_manager = new TwoPLPashaSCCWriteThrough();
                        } else {
                                CHECK(0);
                        }
                }

		return scc_manager;
	}
};

} // namespace star

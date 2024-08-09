//
// Created by Yi Lu on 7/17/18.
//

#pragma once

#include "cxlalloc.h"
#include <glog/logging.h>
#include <string>

namespace star
{
class CXLMemory {
    public:
        static constexpr uint64_t default_cxl_mem_size = (((1024 * 1024 * 1024) + 64 * 1024) * (uint64_t)31);

        CXLMemory(uint64_t threads_num_per_host, uint64_t hosts_num, uint64_t host_id)
                : threads_num_per_host(threads_num_per_host)
                , hosts_num(hosts_num)
                , host_id(host_id)
        {
        }

        void init_cxlalloc_for_given_thread(uint64_t thread_id)
        {
                cxlalloc_init("SS", default_cxl_mem_size, thread_id, threads_num_per_host * hosts_num, host_id, hosts_num);
                LOG(INFO) << "cxlalloc initialized for thread " << thread_id << " on host " << host_id;
        }

    private:
        uint64_t threads_num_per_host;
        uint64_t hosts_num;
        uint64_t host_id;
};
}
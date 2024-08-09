//
// Created by Yibo Huang on 8/8/24.
//

#pragma once

#include "cxlalloc.h"
#include <glog/logging.h>

namespace star
{

class CXLMemory {
    public:
        static constexpr uint64_t default_cxl_mem_size = (((1024 * 1024 * 1024) + 64 * 1024) * (uint64_t)31);

        static void init_cxlalloc_for_given_thread(uint64_t threads_num_per_host, uint64_t thread_id, uint64_t hosts_num, uint64_t host_id)
        {
                cxlalloc_init("SS", default_cxl_mem_size, thread_id, threads_num_per_host * hosts_num, host_id, hosts_num);
                LOG(INFO) << "cxlalloc initialized for thread " << thread_id << " on host " << host_id;
        }

        static void *cxlalloc_malloc_wrapper(uint64_t size)
        {
                return cxlalloc_malloc(size);
        }
};

}
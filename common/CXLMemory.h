//
// Created by Yibo Huang on 8/8/24.
//

#pragma once

#include <atomic>

#include "cxlalloc.h"
#include <glog/logging.h>

namespace star
{

class CXLMemory {
    public:
        // statistics
        enum {
                TOTAL_USAGE,
                TOTAL_HW_CC_USAGE,
                INDEX_USAGE,
                METADATA_USAGE,
                DATA_USAGE,
                TRANSPORT_USAGE,
                MISC_USAGE,
                INDEX_ALLOCATION,
                METADATA_ALLOCATION,
                DATA_ALLOCATION,
                TRANSPORT_ALLOCATION,
                MISC_ALLOCATION,
                INDEX_FREE,
                METADATA_FREE,
                DATA_FREE,
                TRANSPORT_FREE,
                MISC_FREE
        };

        static constexpr uint64_t default_cxl_mem_size = (((1024 * 1024 * 1024) + 64 * 1024) * (uint64_t)31);

        static constexpr uint64_t cxl_transport_root_index = 0;
        static constexpr uint64_t cxl_data_migration_root_index = 1;
        static constexpr uint64_t cxl_lru_trackers_root_index = 2;
        static constexpr uint64_t cxl_global_epoch_root_index = 3;

        void init_cxlalloc_for_given_thread(uint64_t threads_num_per_host, uint64_t thread_id, uint64_t hosts_num, uint64_t host_id)
        {
                cxlalloc_init_backend("ivshmem");
                cxlalloc_init("SS", default_cxl_mem_size, thread_id + threads_num_per_host * host_id, threads_num_per_host * hosts_num, host_id, hosts_num);
                LOG(INFO) << "cxlalloc initialized for thread " << thread_id 
                        << " (global ID = " << thread_id + threads_num_per_host * host_id 
                        << ") on host " << host_id;
        }

        void *cxlalloc_malloc_wrapper(uint64_t size, int category, uint64_t metadata_size = 0, uint64_t data_size = 0)
        {
                // collect statistics
                switch (category) {
                case INDEX_ALLOCATION:
                        size_total_hw_cc_usage.fetch_add(size);
                        size_index_usage.fetch_add(size);
                        break;
                case DATA_ALLOCATION:
                        size_total_hw_cc_usage.fetch_add(metadata_size);
                        size_metadata_usage.fetch_add(metadata_size);
                        size_data_usage.fetch_add(data_size);
                        break;
                case TRANSPORT_ALLOCATION:
                        size_transport_usage.fetch_add(size);
                        break;
                case MISC_ALLOCATION:
                        size_total_hw_cc_usage.fetch_add(size);
                        size_misc_usage.fetch_add(size);
                        break;
                default:
                        CHECK(0);
                }

                return cxlalloc_malloc(size);
        }

        void cxlalloc_free_wrapper(void *ptr, uint64_t size, int category, uint64_t metadata_size = 0, uint64_t data_size = 0)
        {
                // collect statistics
                switch (category) {
                case INDEX_FREE:
                        size_total_hw_cc_usage.fetch_sub(size);
                        size_index_usage.fetch_sub(size);
                        break;
                case DATA_FREE:
                        size_total_hw_cc_usage.fetch_sub(metadata_size);
                        size_metadata_usage.fetch_sub(metadata_size);
                        size_data_usage.fetch_sub(data_size);
                        break;
                case TRANSPORT_FREE:
                        size_transport_usage.fetch_sub(size);
                        break;
                case MISC_FREE:
                        size_total_hw_cc_usage.fetch_sub(size);
                        size_misc_usage.fetch_sub(size);
                        break;
                default:
                        CHECK(0);
                }
        }

        static void commit_shared_data_initialization(uint64_t root_index, void *shared_data)
        {
                cxlalloc_set_root(root_index, shared_data);
        }

        static void wait_and_retrieve_cxl_shared_data(uint64_t root_index, void **shared_data)
        {
                void *addr = NULL;
                while (true) {
                        addr = cxlalloc_get_root(root_index);
                        if (addr)
                                break;
                }

                *shared_data = addr;
        }

        uint64_t get_stats(int category)
        {
                switch (category) {
                case INDEX_USAGE:
                        return size_index_usage;
                case METADATA_USAGE:
                        return size_metadata_usage;
                case DATA_USAGE:
                        return size_data_usage;
                case TRANSPORT_USAGE:
                        return size_transport_usage;
                case MISC_USAGE:
                        return size_misc_usage;
                case TOTAL_HW_CC_USAGE:
                        return size_total_hw_cc_usage;
                case TOTAL_USAGE:
                        return size_index_usage + size_metadata_usage + size_data_usage + size_transport_usage + size_misc_usage;      // does not need to be consistent
                default:
                        CHECK(0);
                }
        }

        void print_stats()
        {
                LOG(INFO) << "local CXL memory usage:"
                          << " size_index_usage: " << get_stats(INDEX_USAGE)
                          << " size_metadata_usage: " << get_stats(METADATA_USAGE)
                          << " size_data_usage: " << get_stats(DATA_USAGE)
                          << " size_transport_usage: " << get_stats(TRANSPORT_USAGE)
                          << " size_misc_usage: " << get_stats(MISC_USAGE)
                          << " total_size_hw_cc_usage: " << get_stats(TOTAL_HW_CC_USAGE)
                          << " total_usage: " << get_stats(TOTAL_USAGE);
        }

    private:
        std::atomic<uint64_t> size_index_usage{ 0 };
        std::atomic<uint64_t> size_metadata_usage{ 0 };
        std::atomic<uint64_t> size_data_usage{ 0 };
        std::atomic<uint64_t> size_transport_usage{ 0 };
        std::atomic<uint64_t> size_misc_usage{ 0 };

        std::atomic<uint64_t> size_total_hw_cc_usage{ 0 };
};

extern CXLMemory cxl_memory;

}

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
                INDEX_USAGE,
                METADATA_USAGE,
                DATA_USAGE,
                TRANSPORT_USAGE,
                INDEX_ALLOCATION,
                METADATA_ALLOCATION,
                DATA_ALLOCATION,
                TRANSPORT_ALLOCATION,
                INDEX_FREE,
                METADATA_FREE,
                DATA_FREE,
                TRANSPORT_FREE
        };

        static constexpr uint64_t default_cxl_mem_size = (((1024 * 1024 * 1024) + 64 * 1024) * (uint64_t)31);

        static constexpr uint64_t cxl_transport_root_index = 0;
        static constexpr uint64_t cxl_data_migration_root_index = 1;
        static constexpr uint64_t cxl_lru_trackers_root_index = 2;

        static constexpr uint64_t minimal_cxlalloc_size = 512;

        void init_cxlalloc_for_given_thread(uint64_t threads_num_per_host, uint64_t thread_id, uint64_t hosts_num, uint64_t host_id)
        {
                cxlalloc_init_backend("ivshmem", false);
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
                        size_index_alloc.fetch_add(size);
                        break;
                case DATA_ALLOCATION:
                        size_metadata_alloc.fetch_add(metadata_size);
                        size_data_alloc.fetch_add(data_size);
                        break;
                case TRANSPORT_ALLOCATION:
                        size_transport_alloc.fetch_add(size);
                        break;
                default:
                        CHECK(0);
                }

                // unfortunately, our allocator has bug
                if (size < minimal_cxlalloc_size)
                        size = minimal_cxlalloc_size;
                return cxlalloc_malloc(size);
        }

        void cxlalloc_free_wrapper(void *ptr, uint64_t size, int category, uint64_t metadata_size = 0, uint64_t data_size = 0)
        {
                // collect statistics
                switch (category) {
                case INDEX_FREE:
                        size_index_free.fetch_add(size);
                        break;
                case DATA_FREE:
                        size_metadata_free.fetch_add(metadata_size);
                        size_data_free.fetch_add(data_size);
                        break;
                case TRANSPORT_FREE:
                        size_transport_free.fetch_add(size);
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
                        return size_index_alloc - size_index_free;
                case METADATA_USAGE:
                        return size_metadata_alloc - size_metadata_free;
                case DATA_USAGE:
                        return size_data_alloc - size_data_free;
                case TRANSPORT_USAGE:
                        return size_transport_alloc - size_transport_free;
                case INDEX_ALLOCATION:
                        return size_index_alloc;
                case METADATA_ALLOCATION:
                        return size_metadata_alloc;
                case DATA_ALLOCATION:
                        return size_data_alloc;
                case TRANSPORT_ALLOCATION:
                        return size_transport_alloc;
                case INDEX_FREE:
                        return size_index_free;
                case METADATA_FREE:
                        return size_metadata_free;
                case DATA_FREE:
                        return size_data_free;
                case TRANSPORT_FREE:
                        return size_transport_free;
                default:
                        CHECK(0);
                }
        }

        void print_stats()
        {
                LOG(INFO) << "local CXL memory usage:"
                          << " size_index_usage: " << size_index_alloc - size_index_free
                          << " size_metadata_usage: " << size_metadata_alloc - size_metadata_free
                          << " size_data_usage: " << size_data_alloc - size_data_free
                          << " size_transport_usage: " << size_transport_alloc - size_transport_free;
        }

    private:
        std::atomic<uint64_t> size_index_alloc{ 0 };
        std::atomic<uint64_t> size_metadata_alloc{ 0 };
        std::atomic<uint64_t> size_data_alloc{ 0 };
        std::atomic<uint64_t> size_transport_alloc{ 0 };

        std::atomic<uint64_t> size_index_free{ 0 };
        std::atomic<uint64_t> size_metadata_free{ 0 };
        std::atomic<uint64_t> size_data_free{ 0 };
        std::atomic<uint64_t> size_transport_free{ 0 };
};

extern CXLMemory cxl_memory;

}

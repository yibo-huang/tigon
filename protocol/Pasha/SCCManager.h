//
// Software Cache-Coherence Manager
//

#pragma once

#include <stdint.h>
#include <atomic>
#include <xmmintrin.h>
#include <glog/logging.h>

/*
 * memory ordering:
 * clflush follows the TSO order in x86.
 * clwb is ordered only by store-fencing operations and is implicitly ordered
 * with older stores executed by the logical processor to the same address.
 * 
 * fortunately, we do not need to deal with this as the ordering
 * will be ensured by the locking primitives
 */
namespace star
{

class SCCManager {
    public:
        static constexpr uint64_t max_metadata_size_in_bytes = 8;

        virtual void init_scc_metadata(void *scc_meta, std::size_t cur_host_id) = 0;

        // assuming every function call is protected by a lock so that
        // we do not need to worry about memory ordering
        virtual void do_read(void *scc_meta, std::size_t cur_host_id, void *dst, const void *src, uint64_t size) = 0;
        virtual void do_write(void *scc_meta, std::size_t cur_host_id, void *dst, const void *src, uint64_t size) = 0;

        void print_stats()
        {
                LOG(INFO) << "software cache-coherence statistics:"
                          << " num_clflush: " << num_clflush
                          << " num_clwb: " << num_clwb
                          << " num_cache_hit: " << num_cache_hit
                          << " num_cache_miss: " << num_cache_miss
                          << " cache hit rate: " << 100.0 * num_cache_hit / (num_cache_hit + num_cache_miss) << "%";
        }

    protected:
        static constexpr uint64_t cacheline_size = 64;

        inline void clflush(const void *addr, uint64_t len)
        {
                // statistics
                num_clflush.fetch_add(1);

                /*
                 * Loop through cache-line-size (typically 64B) aligned chunks
                 * covering the given range.
                 */
                for (uint64_t ptr = (uint64_t)addr & ~(cacheline_size - 1); ptr < (uint64_t)addr + len; ptr += cacheline_size) {
                        asm volatile ("clflush (%0)" :: "r"(ptr));
                }

                // make sure clflush completes before memcpy
                _mm_sfence();
        }

        inline void clwb(const void *addr, uint64_t len)
        {
                // statistics
                num_clwb.fetch_add(1);

                /*
                 * Loop through cache-line-size (typically 64B) aligned chunks
                 * covering the given range.
                 */
                for (uint64_t ptr = (uint64_t)addr & ~(cacheline_size - 1); ptr < (uint64_t)addr + len; ptr += cacheline_size) {
                        asm volatile ("clwb (%0)" :: "r"(ptr));
                }

                // make sure clwb completes before memcpy
                _mm_sfence();
        }

        std::atomic<uint64_t> num_clflush{ 0 };
        std::atomic<uint64_t> num_clwb{ 0 };
        std::atomic<uint64_t> num_cache_hit{ 0 };
        std::atomic<uint64_t> num_cache_miss{ 0 };
};

extern SCCManager *scc_manager;

} // namespace star

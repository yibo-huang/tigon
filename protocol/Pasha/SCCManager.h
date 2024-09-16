//
// Software Cache-Coherence Manager
//

#pragma once

#include <stdint.h>
#include <immintrin.h>
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
        // assuming every function call is protected by a lock so that
        // we do not need to worry about memory ordering
        virtual void do_read(void *buffer, const void *src, uint64_t size) = 0;
        virtual void do_write(void *dst, const void *buffer, uint64_t size) = 0;

    protected:
        static constexpr uint64_t cacheline_size = 64;

        static inline void clflush(const void *addr, uint64_t len)
        {
                /*
                 * Loop through cache-line-size (typically 64B) aligned chunks
                 * covering the given range.
                 */
                for (uint64_t ptr = (uint64_t)addr & ~(cacheline_size - 1); ptr < (uint64_t)addr + len; ptr += cacheline_size) {
                        asm volatile ("clflush (%0)" :: "r"(ptr));
                }
        }

        static inline void clwb(const void *addr, uint64_t len)
        {
                /*
                 * Loop through cache-line-size (typically 64B) aligned chunks
                 * covering the given range.
                 */
                for (uint64_t ptr = (uint64_t)addr & ~(cacheline_size - 1); ptr < (uint64_t)addr + len; ptr += cacheline_size) {
                        asm volatile ("clwb (%0)" :: "r"(ptr));
                }
        }
};

extern SCCManager *scc_manager;

} // namespace star

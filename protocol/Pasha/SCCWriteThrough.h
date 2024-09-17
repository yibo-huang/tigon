//
// Software Cache-Coherence Manager
//

#pragma once

#include <cstring>
#include "common/CXLMemory.h"
#include "protocol/Pasha/SCCManager.h"

namespace star
{

/*
 * We maintain one bit for each host in each tuple.
 * Bit being 1 means it is safe to read from local cache,
 * otherwise a cacheline flush / non-temporal load is required.
 */
class SCCWriteThrough : public SCCManager {
    public:
        void init_scc_metadata(void *scc_meta, std::size_t cur_host_id)
        {
                MetaType *meta = reinterpret_cast<MetaType *>(scc_meta);
                clear_all_bits(*meta);
                set_bit(*meta, cur_host_id);
        }

        void do_read(void *scc_meta, std::size_t cur_host_id, void *dst, const void *src, uint64_t size)
        {
                MetaType *meta = reinterpret_cast<MetaType *>(scc_meta);

                if (is_bit_set(*meta, cur_host_id) == false) {
                        clflush(src, size);
                        set_bit(*meta, cur_host_id);

                        // statistics
                        num_cache_miss.fetch_add(1);
                } else {
                        // statistics
                        num_cache_hit.fetch_add(1);
                }

                // do read
                std::memcpy(dst, src, size);
        }

        void do_write(void *scc_meta, std::size_t cur_host_id, void *dst, const void *src, uint64_t size)
        {
                MetaType *meta = reinterpret_cast<MetaType *>(scc_meta);

                // should always hold for 2PL and Sundial
                CHECK(is_bit_set(*meta, cur_host_id) == true);

                // clear all the bits except the current host
                clear_all_bits(*meta);
                set_bit(*meta, cur_host_id);

                // do write
                std::memcpy(dst, src, size);
                clwb(dst, size);
        }
    private:
        using MetaType = uint32_t;
        static constexpr uint64_t bits_per_word = sizeof(MetaType) * 8;

        // Function to set a bit at a given position in the bitmap
        static void set_bit(MetaType &bitmap, int bit_index) {
                CHECK(bit_index < bits_per_word);
                bitmap |= (1 << bit_index);  // Set the specific bit to 1
        }

        // Function to clear a bit at a given position in the bitmap
        static void clear_bit(MetaType &bitmap, int bit_index) {
                CHECK(bit_index < bits_per_word);
                bitmap &= ~(1 << bit_index);  // Clear the specific bit to 0
        }

        static void clear_all_bits(MetaType &bitmap) {
                bitmap = 0;
        }

        // Function to check if a bit is set (returns true if set, false if clear)
        static bool is_bit_set(MetaType &bitmap, int bit_index) {
                CHECK(bit_index < bits_per_word);
                return (bitmap & (1 << bit_index)) != 0;
        }
};

} // namespace star

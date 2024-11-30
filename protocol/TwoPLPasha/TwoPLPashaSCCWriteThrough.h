//
// Software Cache-Coherence Manager
//

#pragma once

#include <cstring>
#include "common/CXLMemory.h"
#include "protocol/Pasha/SCCManager.h"
#include "protocol/TwoPLPasha/TwoPLPashaHelper.h"

namespace star
{

/*
 * We maintain one bit for each host in each tuple.
 * Bit being 1 means it is safe to read from local cache,
 * otherwise a cacheline flush / non-temporal load is required.
 */
class TwoPLPashaSCCWriteThrough : public SCCManager {
    public:
        void init_scc_metadata(void *scc_meta, std::size_t cur_host_id)
        {
                TwoPLPashaMetadataShared *smeta = reinterpret_cast<TwoPLPashaMetadataShared *>(scc_meta);
                std::size_t host_bit_index = 0;

                for (int i = 0; i < TwoPLPashaMetadataShared::scc_bits_num; i++) {
                        host_bit_index = i + TwoPLPashaMetadataShared::scc_bits_base_index;
                        if (i == cur_host_id) {
                                smeta->set_bit(host_bit_index);
                        } else {
                                smeta->clear_bit(host_bit_index);
                        }
                }
        }

        void do_read(void *scc_meta, std::size_t cur_host_id, void *dst, const void *src, uint64_t size)
        {
                TwoPLPashaMetadataShared *smeta = reinterpret_cast<TwoPLPashaMetadataShared *>(scc_meta);
                std::size_t cur_host_bit_index = cur_host_id + TwoPLPashaMetadataShared::scc_bits_base_index;

                if (smeta->is_bit_set(cur_host_bit_index) == false) {
                        clflush(src, size);
                        smeta->set_bit(cur_host_bit_index);

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
                TwoPLPashaMetadataShared *smeta = reinterpret_cast<TwoPLPashaMetadataShared *>(scc_meta);
                std::size_t cur_host_bit_index = cur_host_id + TwoPLPashaMetadataShared::scc_bits_base_index;
                std::size_t host_bit_index = 0;

                // should always hold for 2PL and Sundial
                CHECK(smeta->is_bit_set(cur_host_bit_index) == true);

                // clear all the bits except the current host
                for (int i = 0; i < TwoPLPashaMetadataShared::scc_bits_num; i++) {
                        host_bit_index = i + TwoPLPashaMetadataShared::scc_bits_base_index;
                        if (i == cur_host_id) {
                                smeta->set_bit(host_bit_index);
                        } else {
                                smeta->clear_bit(host_bit_index);
                        }
                }

                // do write
                std::memcpy(dst, src, size);
                clwb(dst, size);
        }
};

} // namespace star

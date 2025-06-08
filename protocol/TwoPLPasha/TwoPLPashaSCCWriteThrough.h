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
        void init_scc_metadata(void *scc_meta, std::size_t cur_host_id) override
        {
                TwoPLPashaMetadataShared *smeta = reinterpret_cast<TwoPLPashaMetadataShared *>(scc_meta);
                std::size_t host_bit_index = 0;

                // clear all the bits except the current host
                smeta->clear_all_scc_bits();
                smeta->set_scc_bit(cur_host_id);
        }

        void do_read(void *scc_meta, std::size_t cur_host_id, void *dst, const void *src, uint64_t size) override
        {
                memcpy(dst, src, size);
        }

        void do_write(void *scc_meta, std::size_t cur_host_id, void *dst, const void *src, uint64_t size) override
        {
                memcpy(dst, src, size);
        }

        void prepare_read(void *scc_meta, std::size_t cur_host_id, void *scc_data, uint64_t size) override
        {
                TwoPLPashaMetadataShared *smeta = reinterpret_cast<TwoPLPashaMetadataShared *>(scc_meta);
                std::size_t cur_host_bit_index = cur_host_id + TwoPLPashaMetadataShared::scc_bits_base_index;

                if (smeta->is_bit_set(cur_host_bit_index) == false) {
                        clflush(scc_data, size);
                        smeta->set_bit(cur_host_bit_index);

                        // statistics
                        num_cache_miss.fetch_add(1);
                } else {
                        // statistics
                        num_cache_hit.fetch_add(1);
                }
        }

        void finish_write(void *scc_meta, std::size_t cur_host_id, void *scc_data, uint64_t size) override
        {
                TwoPLPashaMetadataShared *smeta = reinterpret_cast<TwoPLPashaMetadataShared *>(scc_meta);
                std::size_t cur_host_bit_index = cur_host_id + TwoPLPashaMetadataShared::scc_bits_base_index;

                // should always hold for 2PL and Sundial
                CHECK(smeta->is_bit_set(cur_host_bit_index) == true);

                // clear all the bits except the current host
                smeta->clear_all_scc_bits();
                smeta->set_scc_bit(cur_host_id);

                clwb(scc_data, size);
        }
};

} // namespace star

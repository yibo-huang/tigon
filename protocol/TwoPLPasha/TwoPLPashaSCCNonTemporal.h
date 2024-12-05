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
class TwoPLPashaSCCNonTemporal : public SCCManager {
    public:
        void init_scc_metadata(void *scc_meta, std::size_t cur_host_id) override
        {
        }

        void do_read(void *scc_meta, std::size_t cur_host_id, void *dst, const void *src, uint64_t size) override
        {
                // do nothing
        }

        void do_write(void *scc_meta, std::size_t cur_host_id, void *dst, const void *src, uint64_t size) override
        {
                // do nothing
        }

        void prepare_read(void *scc_meta, std::size_t cur_host_id, void *scc_data, uint64_t size) override
        {
                clflush(scc_data, size);
        }

        void finish_write(void *scc_meta, std::size_t cur_host_id, void *scc_data, uint64_t size) override
        {
                clwb(scc_data, size);
        }
};

} // namespace star

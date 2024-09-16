//
// Software Cache-Coherence Manager
//

#pragma once

#include <cstring>
#include "protocol/Pasha/SCCManager.h"

namespace star
{

class SCCNonTemporal : public SCCManager {
    public:
        void *create_scc_metadata(std::size_t cur_host_id)
        {
                return nullptr;
        }

        void do_read(void *scc_meta, std::size_t cur_host_id, void *dst, const void *src, uint64_t size)
        {
                clflush(src, size);
                std::memcpy(dst, src, size);
        }

        void do_write(void *scc_meta, std::size_t cur_host_id, void *dst, const void *src, uint64_t size)
        {
                std::memcpy(dst, src, size);
                clwb(dst, size);
        }
};

} // namespace star

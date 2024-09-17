//
// Software Cache-Coherence Manager
//

#pragma once

#include <cstring>
#include "protocol/Pasha/SCCManager.h"

namespace star
{

class SCCNoOP : public SCCManager {
    public:
        void init_scc_metadata(void *scc_meta, std::size_t cur_host_id)
        {}

        void do_read(void *scc_meta, std::size_t cur_host_id, void *dst, const void *src, uint64_t size)
        {
                std::memcpy(dst, src, size);
        }

        void do_write(void *scc_meta, std::size_t cur_host_id, void *dst, const void *src, uint64_t size)
        {
                std::memcpy(dst, src, size);
        }
};

} // namespace star

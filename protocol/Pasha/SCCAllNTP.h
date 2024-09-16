//
// Software Cache-Coherence Manager
//

#pragma once

#include "protocol/Pasha/SCCManager.h"

namespace star
{

class SCCAllNTP : public SCCManager {
    public:
        void do_read(void *buffer, const void *src, uint64_t size)
        {
                clflush(src, size);
                std::memcpy(buffer, src, size);
        }

        void do_write(void *dst, const void *buffer, uint64_t size)
        {
                std::memcpy(dst, buffer, size);
                clwb(dst, size);
        }
};

} // namespace star

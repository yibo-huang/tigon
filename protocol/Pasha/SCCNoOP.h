//
// Software Cache-Coherence Manager
//

#pragma once

#include "protocol/Pasha/SCCManager.h"

namespace star
{

class SCCNoOP : public SCCManager {
    public:
        void do_read(void *dst, const void *src, uint64_t size)
        {
                std::memcpy(dst, src, size);
        }

        void do_write(void *dst, const void *src, uint64_t size)
        {
                std::memcpy(dst, src, size);
        }
};

} // namespace star

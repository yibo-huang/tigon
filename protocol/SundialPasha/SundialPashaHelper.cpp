#include "protocol/SundialPasha/SundialPashaHelper.h"

namespace star {

uint64_t SundialPashaMetadataLocalInit(bool is_tuple_valid = true)
{
	auto lmeta = new SundialPashaMetadataLocal();
        lmeta->is_valid = is_tuple_valid;
        return reinterpret_cast<uint64_t>(lmeta);
}

SundialPashaHelper *sundial_pasha_global_helper = nullptr;

}

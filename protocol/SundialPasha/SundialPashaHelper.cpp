#include "protocol/SundialPasha/SundialPashaHelper.h"

namespace star {

uint64_t SundialPashaMetadataLocalInit()
{
	return reinterpret_cast<uint64_t>(new SundialPashaMetadataLocal());
}

SundialPashaHelper *sundial_pasha_global_helper = nullptr;

}

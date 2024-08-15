#include "protocol/SundialPasha/SundialPashaHelper.h"

namespace star {

uint64_t SundialPashaMetadataLocalInit()
{
	return reinterpret_cast<uint64_t>(new SundialPashaMetadataLocal());
}

uint64_t SundialPashaMetadataSharedInit()
{
	return reinterpret_cast<uint64_t>(new SundialPashaMetadataShared());
}

SundialPashaHelper global_helper;

}

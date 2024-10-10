#include "protocol/TwoPLPasha/TwoPLPashaHelper.h"

namespace star {

uint64_t TwoPLPashaMetadataLocalInit()
{
	return reinterpret_cast<uint64_t>(new TwoPLPashaMetadataLocal());
}

TwoPLPashaHelper twopl_pasha_global_helper;

}

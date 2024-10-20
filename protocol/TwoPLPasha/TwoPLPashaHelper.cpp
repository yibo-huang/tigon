#include "protocol/TwoPLPasha/TwoPLPashaHelper.h"

namespace star {

uint64_t TwoPLPashaMetadataLocalInit(bool is_tuple_valid = true)
{
	auto lmeta = new TwoPLPashaMetadataLocal();
        lmeta->is_valid = is_tuple_valid;
        return reinterpret_cast<uint64_t>(lmeta);
}

TwoPLPashaHelper *twopl_pasha_global_helper = nullptr;

}

#include "protocol/TwoPLPasha/TwoPLPashaHelper.h"

namespace star {

uint64_t TwoPLPashaMetadataLocalInit()
{
        auto *lmeta = new TwoPLPashaMetadataLocal();
        pthread_spin_init(&lmeta->latch, PTHREAD_PROCESS_SHARED);
	return reinterpret_cast<uint64_t>(lmeta);
}

TwoPLPashaHelper twopl_pasha_global_helper;

}

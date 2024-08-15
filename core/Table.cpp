#include "core/Table.h"

namespace star {

void tid_check()
{
	if (do_tid_check) {
		if (tid == std::numeric_limits<uint64_t>::max()) {
			tid = tid_hasher(std::this_thread::get_id());
		} else {
			DCHECK(tid_hasher(std::this_thread::get_id()) == tid);
		}
	}
}

}

#include "BTreeOLC_CXL.h"

namespace btreeolc_cxl
{

uint32_t AllocateRWSpinLatchThreadId()
{
	static std::atomic<uint32_t> RWSpinLatchThreadIdAlloc{ 0 };
	return RWSpinLatchThreadIdAlloc.fetch_add(1) + 1;
}

thread_local uint32_t RWSpinLatchThreadId = AllocateRWSpinLatchThreadId();

}
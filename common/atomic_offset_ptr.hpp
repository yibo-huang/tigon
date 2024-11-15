#pragma once
#ifndef COMMON_CPP_ATOMIC_OFFSET_PTR_HPP_
#define COMMON_CPP_ATOMIC_OFFSET_PTR_HPP_

// from:
// https://github.com/microsoft/L4/blob/master/inc/L4/Utils/AtomicOffsetPtr.h

#include <boost/interprocess/offset_ptr.hpp>
#include <boost/version.hpp>
#include <cstdint>

// AtomicOffsetPtr provides a way to atomically update the offset pointer.
// The current boost::interprocess::offset_ptr cannot be used with std::atomic<>
// because the class is not trivially copyable. AtomicOffsetPtr borrows the same
// concept to calculate the pointer address based on the offset
// (boost::interprocess::ipcdetail::offset_ptr_to* functions are reused). Note
// that ->, *, copy/assignment operators are not implemented intentionally so
// that the user (inside this library) is aware of what he is intended to do
// without accidentally incurring any performance hits.
template <typename T> class AtomicOffsetPtr {
    public:
	AtomicOffsetPtr()
		: m_offset(1)
	{
	}

	AtomicOffsetPtr(const AtomicOffsetPtr &) = delete;
	AtomicOffsetPtr &operator=(const AtomicOffsetPtr &) = delete;

	T *Load(std::memory_order memoryOrder = std::memory_order_acquire) const
	{
		return static_cast<T *>(boost::interprocess::ipcdetail::offset_ptr_to_raw_pointer(this, m_offset.load(memoryOrder)));
	}

	T *load(std::memory_order memoryOrder = std::memory_order_acquire) const
	{
		return static_cast<T *>(boost::interprocess::ipcdetail::offset_ptr_to_raw_pointer(this, m_offset.load(memoryOrder)));
	}

	T *load_relaxed() const
	{
		return static_cast<T *>(boost::interprocess::ipcdetail::offset_ptr_to_raw_pointer(this, m_offset.load(std::memory_order_relaxed)));
	}

	void Store(T *ptr, std::memory_order memoryOrder = std::memory_order_release)
	{
		m_offset.store(boost::interprocess::ipcdetail::offset_ptr_to_offset<std::uintptr_t>(ptr, this), memoryOrder);
	}

	void store(T *ptr, std::memory_order memoryOrder = std::memory_order_release)
	{
		m_offset.store(boost::interprocess::ipcdetail::offset_ptr_to_offset<std::uintptr_t>(ptr, this), memoryOrder);
	}

	void store_relaxed(T *ptr)
	{
		m_offset.store(boost::interprocess::ipcdetail::offset_ptr_to_offset<std::uintptr_t>(ptr, this), std::memory_order_relaxed);
	}

	bool CompareExchangeStrongAcqRel(T *&exp, T *desire)
	{
		std::uint64_t old_off = boost::interprocess::ipcdetail::offset_ptr_to_offset<std::uintptr_t>(exp, this);
		std::uint64_t new_off = boost::interprocess::ipcdetail::offset_ptr_to_offset<std::uintptr_t>(desire, this);
		bool ret = atomic_compare_exchange_strong_explicit(&m_offset, &old_off, new_off, std::memory_order_acq_rel, std::memory_order_acquire);
		if (!ret) {
			exp = reinterpret_cast<T *>(boost::interprocess::ipcdetail::offset_ptr_to_raw_pointer(this, old_off));
			return false;
		}
		return true;
	}

	bool compare_exchange_strong(T *&exp, T *desire, std::memory_order success, std::memory_order fail)
	{
		std::uint64_t old_off = boost::interprocess::ipcdetail::offset_ptr_to_offset<std::uintptr_t>(exp, this);
		std::uint64_t new_off = boost::interprocess::ipcdetail::offset_ptr_to_offset<std::uintptr_t>(desire, this);
		bool ret = atomic_compare_exchange_strong_explicit(&m_offset, &old_off, new_off, success, fail);
		if (!ret) {
			exp = static_cast<T *>(boost::interprocess::ipcdetail::offset_ptr_to_raw_pointer(this, old_off));
			return false;
		}
		return true;
	}

	bool CompareExchangeWeakAcqRel(T *&exp, T *desire)
	{
		std::uint64_t old_off = boost::interprocess::ipcdetail::offset_ptr_to_offset<std::uintptr_t>(exp, this);
		std::uint64_t new_off = boost::interprocess::ipcdetail::offset_ptr_to_offset<std::uintptr_t>(desire, this);
		bool ret = atomic_compare_exchange_weak_explicit(&m_offset, &old_off, new_off, std::memory_order_acq_rel, std::memory_order_acquire);
		if (!ret) {
			exp = reinterpret_cast<T *>(boost::interprocess::ipcdetail::offset_ptr_to_raw_pointer(this, old_off));
			return false;
		}
		return true;
	}

	bool compare_exchange_weak(T *&exp, T *desire, std::memory_order success, std::memory_order fail)
	{
		std::uint64_t old_off = boost::interprocess::ipcdetail::offset_ptr_to_offset<std::uintptr_t>(exp, this);
		std::uint64_t new_off = boost::interprocess::ipcdetail::offset_ptr_to_offset<std::uintptr_t>(desire, this);
		bool ret = atomic_compare_exchange_weak_explicit(&m_offset, &old_off, new_off, success, fail);
		if (!ret) {
			exp = static_cast<T *>(boost::interprocess::ipcdetail::offset_ptr_to_raw_pointer(this, old_off));
			return false;
		}
		return true;
	}

	std::atomic<std::uint64_t> m_offset;
};
static_assert(std::is_standard_layout<AtomicOffsetPtr<int> >::value, "");

#endif // COMMON_CPP_ATOMIC_OFFSET_PTR_HPP_

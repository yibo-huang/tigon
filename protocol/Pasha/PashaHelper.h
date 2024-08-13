//
// Created by Xinjing Zhou Lu on 04/26/22.
//

#pragma once

#include <atomic>
#include <list>
#include <tuple>
#include <memory>

#include "glog/logging.h"

namespace star
{

struct PashaMetadata {
	std::atomic<uint64_t> latch{ 0 };
	void lock()
	{
retry:
		auto v = latch.load();
		if (v == 0) {
			if (latch.compare_exchange_strong(v, 1))
				return;
			goto retry;
		} else {
			goto retry;
		}
	}

	void unlock()
	{
		DCHECK(latch.load() == 1);
		latch.store(0);
	}

	uint64_t wts{ 0 };
	uint64_t rts{ 0 };
	uint64_t owner{ 0 };
	// std::list<LockWaiterMeta*> waitlist;
};

uint64_t PashaMetadataInit()
{
	return reinterpret_cast<uint64_t>(new PashaMetadata());
}

class PashaHelper {
    public:
	using MetaDataType = std::atomic<uint64_t>;

	// static uint64_t read(const std::tuple<MetaDataType *, void *> &row,
	//                      void *dest, std::size_t size) {

	//   MetaDataType &tid = *std::get<0>(row);
	//   void *src = std::get<1>(row);

	//   // read from a consistent view. read the value even it's locked by others.
	//   // abort in read validation phase
	//   uint64_t tid_;
	//   do {
	//     tid_ = tid.load();
	//     std::memcpy(dest, src, size);
	//   } while (tid_ != tid.load());

	//   return remove_lock_bit(tid_);
	// }

	static uint64_t get_or_install_meta(std::atomic<uint64_t> &ptr)
	{
retry:
		auto v = ptr.load();
		if (v != 0) {
			return v;
		}
		auto meta_ptr = PashaMetadataInit();
		if (ptr.compare_exchange_strong(v, meta_ptr) == false) {
			delete ((PashaMetadata *)meta_ptr);
			goto retry;
		}
		return meta_ptr;
	}

	// Returns <rts, wts> of the tuple.
	static std::pair<uint64_t, uint64_t> read(const std::tuple<MetaDataType *, void *> &row, void *dest, std::size_t size)
	{
		MetaDataType &meta = *std::get<0>(row);
		PashaMetadata *smeta = reinterpret_cast<PashaMetadata *>(get_or_install_meta(meta));
		DCHECK(smeta != nullptr);
		void *src = std::get<1>(row);

		smeta->lock();
		auto rts = smeta->rts;
		auto wts = smeta->wts;
		std::memcpy(dest, src, size);
		smeta->unlock();

		return std::make_pair(wts, rts);
	}

	// Returns <rts, wts> of the tuple.
	static bool write_lock(const std::tuple<MetaDataType *, void *> &row, std::pair<uint64_t, uint64_t> &rwts, uint64_t transaction_id)
	{
		MetaDataType &meta = *std::get<0>(row);
		PashaMetadata *smeta = reinterpret_cast<PashaMetadata *>(get_or_install_meta(meta));

		bool success = false;
		smeta->lock();
		rwts.first = smeta->wts;
		rwts.second = smeta->rts;
		if (smeta->owner == 0 || smeta->owner == transaction_id) {
			success = true;
			smeta->owner = transaction_id;
		}
		smeta->unlock();

		return success;
	}

	static bool renew_lease(const std::tuple<MetaDataType *, void *> &row, uint64_t wts, uint64_t commit_ts)
	{
		MetaDataType &meta = *std::get<0>(row);
		PashaMetadata *smeta = reinterpret_cast<PashaMetadata *>(get_or_install_meta(meta));

		bool success = false;
		smeta->lock();
		if (wts != smeta->wts || (commit_ts > smeta->rts && smeta->owner != 0)) {
			success = false;
		} else {
			success = true;
			smeta->rts = std::max(smeta->rts, commit_ts);
		}
		smeta->unlock();

		return success;
	}

	static void replica_update(const std::tuple<MetaDataType *, void *> &row, const void *value, std::size_t value_size, uint64_t commit_ts)
	{
		MetaDataType &meta = *std::get<0>(row);
		void *data_ptr = std::get<1>(row);
		PashaMetadata *smeta = reinterpret_cast<PashaMetadata *>(get_or_install_meta(meta));

		smeta->lock();
		DCHECK(smeta->wts == smeta->rts);
		if (commit_ts > smeta->wts) { // Thomas write rule
			smeta->wts = smeta->rts = commit_ts;
			memcpy(data_ptr, value, value_size);
		}
		smeta->unlock();
	}

	static void update(const std::tuple<MetaDataType *, void *> &row, const void *value, std::size_t value_size, uint64_t commit_ts,
			   uint64_t transaction_id)
	{
		MetaDataType &meta = *std::get<0>(row);
		void *data_ptr = std::get<1>(row);
		PashaMetadata *smeta = reinterpret_cast<PashaMetadata *>(get_or_install_meta(meta));

		smeta->lock();
		CHECK(smeta->owner == transaction_id);
		memcpy(data_ptr, value, value_size);
		smeta->wts = smeta->rts = commit_ts;
		smeta->unlock();
	}

	static void update_unlock(const std::tuple<MetaDataType *, void *> &row, const void *value, std::size_t value_size, uint64_t commit_ts,
				  uint64_t transaction_id)
	{
		MetaDataType &meta = *std::get<0>(row);
		void *data_ptr = std::get<1>(row);
		PashaMetadata *smeta = reinterpret_cast<PashaMetadata *>(get_or_install_meta(meta));

		smeta->lock();
		CHECK(smeta->owner == transaction_id);
		memcpy(data_ptr, value, value_size);
		smeta->wts = smeta->rts = commit_ts;
		smeta->owner = 0;
		smeta->unlock();
	}

	static void unlock(const std::tuple<MetaDataType *, void *> &row, uint64_t transaction_id)
	{
		MetaDataType &meta = *std::get<0>(row);
		PashaMetadata *smeta = reinterpret_cast<PashaMetadata *>(get_or_install_meta(meta));

		smeta->lock();
		CHECK(smeta->owner == transaction_id);
		smeta->owner = 0;
		smeta->unlock();
	}

	static bool is_locked(uint64_t value)
	{
		return (value >> LOCK_BIT_OFFSET) & LOCK_BIT_MASK;
	}

	static uint64_t lock(std::atomic<uint64_t> &a)
	{
		uint64_t oldValue, newValue;
		do {
			do {
				oldValue = a.load();
			} while (is_locked(oldValue));
			newValue = (LOCK_BIT_MASK << LOCK_BIT_OFFSET) | oldValue;
		} while (!a.compare_exchange_weak(oldValue, newValue));
		DCHECK(is_locked(oldValue) == false);
		return oldValue;
	}

	static uint64_t lock(std::atomic<uint64_t> &a, bool &success)
	{
		uint64_t oldValue = a.load();

		if (is_locked(oldValue)) {
			success = false;
		} else {
			uint64_t newValue = (LOCK_BIT_MASK << LOCK_BIT_OFFSET) | oldValue;
			success = a.compare_exchange_strong(oldValue, newValue);
		}
		return oldValue;
	}

	static void unlock(std::atomic<uint64_t> &a)
	{
		uint64_t oldValue = a.load();
		DCHECK(is_locked(oldValue));
		uint64_t newValue = remove_lock_bit(oldValue);
		bool ok = a.compare_exchange_strong(oldValue, newValue);
		DCHECK(ok);
	}

	static void unlock(std::atomic<uint64_t> &a, uint64_t newValue)
	{
		uint64_t oldValue = a.load();
		DCHECK(is_locked(oldValue));
		DCHECK(is_locked(newValue) == false);
		bool ok = a.compare_exchange_strong(oldValue, newValue);
		DCHECK(ok);
	}

	static uint64_t remove_lock_bit(uint64_t value)
	{
		return value & ~(LOCK_BIT_MASK << LOCK_BIT_OFFSET);
	}

    public:
	static constexpr int LOCK_BIT_OFFSET = 63;
	static constexpr uint64_t LOCK_BIT_MASK = 0x1ull;
};

} // namespace star
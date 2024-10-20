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

struct SundialMetadata {
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

        bool is_valid{ false };
};

uint64_t SundialMetadataInit(bool is_tuple_valid = true)
{
	auto lmeta = new SundialMetadata();
        lmeta->is_valid = is_tuple_valid;
        return reinterpret_cast<uint64_t>(lmeta);
}

class SundialHelper {
    public:
	using MetaDataType = std::atomic<uint64_t>;

	static uint64_t get_or_install_meta(std::atomic<uint64_t> &ptr)
	{
retry:
		auto v = ptr.load();
		if (v != 0) {
			return v;
		}
		auto meta_ptr = SundialMetadataInit();
		if (ptr.compare_exchange_strong(v, meta_ptr) == false) {
			delete ((SundialMetadata *)meta_ptr);
			goto retry;
		}
		return meta_ptr;
	}

	// Returns <rts, wts> of the tuple.
	static std::pair<uint64_t, uint64_t> read(const std::tuple<MetaDataType *, void *> &row, void *dest, std::size_t size)
	{
		MetaDataType &meta = *std::get<0>(row);
		SundialMetadata *lmeta = reinterpret_cast<SundialMetadata *>(get_or_install_meta(meta));
		void *src = std::get<1>(row);

		lmeta->lock();
		auto rts = lmeta->rts;
		auto wts = lmeta->wts;
		std::memcpy(dest, src, size);
		lmeta->unlock();

		return std::make_pair(wts, rts);
	}

	// Returns <rts, wts> of the tuple.
	static bool write_lock(const std::tuple<MetaDataType *, void *> &row, std::pair<uint64_t, uint64_t> &rwts, uint64_t transaction_id)
	{
		MetaDataType &meta = *std::get<0>(row);
		SundialMetadata *lmeta = reinterpret_cast<SundialMetadata *>(get_or_install_meta(meta));
		bool success = false;

		lmeta->lock();
                if (lmeta->is_valid == false) {
                        goto out_lmeta_unlock;
                }
		rwts.first = lmeta->wts;
		rwts.second = lmeta->rts;
		if (lmeta->owner == 0 || lmeta->owner == transaction_id) {
			success = true;
			lmeta->owner = transaction_id;
		}

out_lmeta_unlock:
		lmeta->unlock();
		return success;
	}

	static bool renew_lease(const std::tuple<MetaDataType *, void *> &row, uint64_t wts, uint64_t commit_ts)
	{
		MetaDataType &meta = *std::get<0>(row);
		SundialMetadata *lmeta = reinterpret_cast<SundialMetadata *>(get_or_install_meta(meta));
		bool success = false;

		lmeta->lock();
                if (lmeta->is_valid == false) {
                        goto out_lmeta_unlock;
                }
		if (wts != lmeta->wts || (commit_ts > lmeta->rts && lmeta->owner != 0)) {
			success = false;
		} else {
			success = true;
			lmeta->rts = std::max(lmeta->rts, commit_ts);
		}

out_lmeta_unlock:
		lmeta->unlock();
		return success;
	}

	static void replica_update(const std::tuple<MetaDataType *, void *> &row, const void *value, std::size_t value_size, uint64_t commit_ts)
	{
		CHECK(0);
	}

	static void update(const std::tuple<MetaDataType *, void *> &row, const void *value, std::size_t value_size, uint64_t commit_ts,
			   uint64_t transaction_id)
	{
		MetaDataType &meta = *std::get<0>(row);
		SundialMetadata *lmeta = reinterpret_cast<SundialMetadata *>(get_or_install_meta(meta));
                void *data_ptr = std::get<1>(row);

		lmeta->lock();
                CHECK(lmeta->is_valid == true);
		CHECK(lmeta->owner == transaction_id);
		memcpy(data_ptr, value, value_size);
		lmeta->wts = lmeta->rts = commit_ts;
		lmeta->unlock();
	}

	static void update_unlock(const std::tuple<MetaDataType *, void *> &row, const void *value, std::size_t value_size, uint64_t commit_ts,
				  uint64_t transaction_id)
	{
		MetaDataType &meta = *std::get<0>(row);
		SundialMetadata *lmeta = reinterpret_cast<SundialMetadata *>(get_or_install_meta(meta));
		void *data_ptr = std::get<1>(row);

		lmeta->lock();
                CHECK(lmeta->is_valid == true);
		CHECK(lmeta->owner == transaction_id);
		memcpy(data_ptr, value, value_size);
		lmeta->wts = lmeta->rts = commit_ts;
		lmeta->owner = 0;
		lmeta->unlock();
	}

	static void unlock(const std::tuple<MetaDataType *, void *> &row, uint64_t transaction_id)
	{
		MetaDataType &meta = *std::get<0>(row);
		SundialMetadata *lmeta = reinterpret_cast<SundialMetadata *>(get_or_install_meta(meta));

		lmeta->lock();
                CHECK(lmeta->is_valid == true);
		CHECK(lmeta->owner == transaction_id);
		lmeta->owner = 0;
		lmeta->unlock();
	}

        static void mark_tuple_as_valid(const std::tuple<MetaDataType *, void *> &row)
        {
                MetaDataType &meta = *std::get<0>(row);
		SundialMetadata *lmeta = reinterpret_cast<SundialMetadata *>(meta.load());

                lmeta->lock();
                CHECK(lmeta->is_valid == false);
                lmeta->is_valid = true;
                lmeta->unlock();
        }

};

} // namespace star
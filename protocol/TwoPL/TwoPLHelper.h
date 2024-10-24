//
// Created by Yi Lu on 9/11/18.
//

#pragma once

#include <atomic>
#include <cstring>
#include <glog/logging.h>
#include <tuple>

namespace star
{

struct TwoPLMetadata {
        TwoPLMetadata()
                : tid(0)
                , is_valid(false)
        {
                // this spinlock will only be shared within a single process
                pthread_spin_init(&latch, PTHREAD_PROCESS_PRIVATE);
        }

        void lock()
	{
                pthread_spin_lock(&latch);
	}

	void unlock()
	{
		pthread_spin_unlock(&latch);
	}

        pthread_spinlock_t latch;

	uint64_t tid{ 0 };

        bool is_valid{ false };
};

uint64_t TwoPLMetadataInit(bool is_tuple_valid = true)
{
	auto lmeta = new TwoPLMetadata();
        lmeta->is_valid = is_tuple_valid;
        return reinterpret_cast<uint64_t>(lmeta);
}

class TwoPLHelper {
    public:
	using MetaDataType = std::atomic<uint64_t>;

	static uint64_t read(const std::tuple<MetaDataType *, void *> &row, void *dest, std::size_t size)
	{
                MetaDataType &meta = *std::get<0>(row);
                TwoPLMetadata *lmeta = reinterpret_cast<TwoPLMetadata *>(meta.load());
                uint64_t tid_ = 0;

                lmeta->lock();
                CHECK(lmeta->is_valid == true);
                void *src = std::get<1>(row);
                std::memcpy(dest, src, size);
                tid_ = lmeta->tid;
                lmeta->unlock();

		return remove_lock_bit(tid_);
	}

        static void update(const std::tuple<MetaDataType *, void *> &row, const void *value, std::size_t value_size)
	{
		MetaDataType &meta = *std::get<0>(row);
                TwoPLMetadata *lmeta = reinterpret_cast<TwoPLMetadata *>(meta.load());

		lmeta->lock();
                CHECK(lmeta->is_valid == true);
                void *data_ptr = std::get<1>(row);
                std::memcpy(data_ptr, value, value_size);
		lmeta->unlock();
	}

	/**
	 * [write lock bit (1) |  read lock bit (9) -- 512 - 1 locks | seq id  (54) ]
	 *
	 */

	static bool is_read_locked(uint64_t value)
	{
		return value & (READ_LOCK_BIT_MASK << READ_LOCK_BIT_OFFSET);
	}

	static bool is_write_locked(uint64_t value)
	{
		return value & (WRITE_LOCK_BIT_MASK << WRITE_LOCK_BIT_OFFSET);
	}

	static uint64_t read_lock_num(uint64_t value)
	{
		return (value >> READ_LOCK_BIT_OFFSET) & READ_LOCK_BIT_MASK;
	}

	static uint64_t read_lock_max()
	{
		return READ_LOCK_BIT_MASK;
	}

	static uint64_t read_lock(std::atomic<uint64_t> &meta, bool &success)
	{
                TwoPLMetadata *lmeta = reinterpret_cast<TwoPLMetadata *>(meta.load());
                uint64_t old_value = 0, new_value = 0;

                lmeta->lock();
                if (lmeta->is_valid == false) {
                        success = false;
                        goto out_unlock_lmeta;
                }

                old_value = lmeta->tid;

                // can we get the lock?
                if (is_write_locked(old_value) || read_lock_num(old_value) == read_lock_max()) {
                        success = false;
                        goto out_unlock_lmeta;
                }

                // OK, we can get the lock
                new_value = old_value + (1ull << READ_LOCK_BIT_OFFSET);
                lmeta->tid = new_value;
                success = true;

out_unlock_lmeta:
                lmeta->unlock();
		return remove_lock_bit(old_value);
	}

        static uint64_t write_lock(std::atomic<uint64_t> &meta, bool &success)
	{
                TwoPLMetadata *lmeta = reinterpret_cast<TwoPLMetadata *>(meta.load());
                uint64_t old_value = 0, new_value = 0;

                lmeta->lock();
                if (lmeta->is_valid == false) {
                        success = false;
                        goto out_unlock_lmeta;
                }

                old_value = lmeta->tid;

                // can we get the lock?
                if (is_read_locked(old_value) || is_write_locked(old_value)) {
                        success = false;
                        goto out_unlock_lmeta;
                }

                // OK, we can get the lock
                new_value = old_value + (WRITE_LOCK_BIT_MASK << WRITE_LOCK_BIT_OFFSET);
                lmeta->tid = new_value;
                success = true;

out_unlock_lmeta:
                lmeta->unlock();
		return remove_lock_bit(old_value);
	}

	static uint64_t write_lock(std::atomic<uint64_t> &meta)
	{
		uint64_t ret = 0;
                bool success = false;

                while (true) {
                        ret = write_lock(meta, success);
                        if (success == true) {
                                break;
                        }
                }

                return ret;
	}

	static void read_lock_release(std::atomic<uint64_t> &meta)
	{
                TwoPLMetadata *lmeta = reinterpret_cast<TwoPLMetadata *>(meta.load());
                uint64_t old_value = 0, new_value = 0;

                lmeta->lock();
                CHECK(lmeta->is_valid == true);
                old_value = lmeta->tid;
                DCHECK(is_read_locked(old_value));
                DCHECK(!is_write_locked(old_value));
                new_value = old_value - (1ull << READ_LOCK_BIT_OFFSET);
                lmeta->tid = new_value;
                lmeta->unlock();
	}

	static void write_lock_release(std::atomic<uint64_t> &meta)
	{
                TwoPLMetadata *lmeta = reinterpret_cast<TwoPLMetadata *>(meta.load());
                uint64_t old_value = 0, new_value = 0;

                lmeta->lock();
                CHECK(lmeta->is_valid == true);
                old_value = lmeta->tid;
                DCHECK(!is_read_locked(old_value));
                DCHECK(is_write_locked(old_value));
                new_value = old_value - (1ull << WRITE_LOCK_BIT_OFFSET);
                lmeta->tid = new_value;
                lmeta->unlock();
	}

	static void write_lock_release(std::atomic<uint64_t> &meta, uint64_t new_value)
	{
                TwoPLMetadata *lmeta = reinterpret_cast<TwoPLMetadata *>(meta.load());
                uint64_t old_value = 0;

                lmeta->lock();
                CHECK(lmeta->is_valid == true);

                old_value = lmeta->tid;
                DCHECK(!is_read_locked(old_value));
                DCHECK(is_write_locked(old_value));
                DCHECK(!is_read_locked(new_value));
                DCHECK(!is_write_locked(new_value));
                lmeta->tid = new_value;
                lmeta->unlock();
	}

        static void mark_tuple_as_valid(std::atomic<uint64_t> &meta)
        {
                TwoPLMetadata *lmeta = reinterpret_cast<TwoPLMetadata *>(meta.load());

                lmeta->lock();
                CHECK(lmeta->is_valid == false);
                lmeta->is_valid = true;
                lmeta->unlock();
        }

	static uint64_t remove_lock_bit(uint64_t value)
	{
		return value & ~(LOCK_BIT_MASK << LOCK_BIT_OFFSET);
	}

	static uint64_t remove_read_lock_bit(uint64_t value)
	{
		return value & ~(READ_LOCK_BIT_MASK << READ_LOCK_BIT_OFFSET);
	}

	static uint64_t remove_write_lock_bit(uint64_t value)
	{
		return value & ~(WRITE_LOCK_BIT_MASK << WRITE_LOCK_BIT_OFFSET);
	}

    public:
	static constexpr int LOCK_BIT_OFFSET = 54;
	static constexpr uint64_t LOCK_BIT_MASK = 0x3ffull;

	static constexpr int READ_LOCK_BIT_OFFSET = 54;
	static constexpr uint64_t READ_LOCK_BIT_MASK = 0x1ffull;

	static constexpr int WRITE_LOCK_BIT_OFFSET = 63;
	static constexpr uint64_t WRITE_LOCK_BIT_MASK = 0x1ull;
};
} // namespace star
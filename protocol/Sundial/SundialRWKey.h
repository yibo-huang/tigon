//
// Created by Xinjing Zhou Lu on 04/26/22.
//

#pragma once

#include <algorithm>
#include <atomic>
#include <thread>

#include <glog/logging.h>

namespace star
{

class SundialRWKey {
    public:
	// local index read bit

	void set_local_index_read_bit()
	{
		clear_local_index_read_bit();
		bitvec |= LOCAL_INDEX_READ_BIT_MASK << LOCAL_INDEX_READ_BIT_OFFSET;
	}

	void clear_local_index_read_bit()
	{
		bitvec &= ~(LOCAL_INDEX_READ_BIT_MASK << LOCAL_INDEX_READ_BIT_OFFSET);
	}

	uint64_t get_local_index_read_bit() const
	{
		return (bitvec >> LOCAL_INDEX_READ_BIT_OFFSET) & LOCAL_INDEX_READ_BIT_MASK;
	}

	// read request bit

	void set_read_request_bit()
	{
		clear_read_request_bit();
		bitvec |= READ_REQUEST_BIT_MASK << READ_REQUEST_BIT_OFFSET;
	}

	void clear_read_request_bit()
	{
		bitvec &= ~(READ_REQUEST_BIT_MASK << READ_REQUEST_BIT_OFFSET);
	}

	uint64_t get_read_request_bit() const
	{
		return (bitvec >> READ_REQUEST_BIT_OFFSET) & READ_REQUEST_BIT_MASK;
	}

	// write request bit

	void set_write_request_bit()
	{
		clear_write_request_bit();
		bitvec |= WRITE_REQUEST_BIT_MASK << WRITE_REQUEST_BIT_OFFSET;
	}

	void clear_write_request_bit()
	{
		bitvec &= ~(WRITE_REQUEST_BIT_MASK << WRITE_REQUEST_BIT_OFFSET);
	}

	uint64_t get_write_request_bit() const
	{
		return (bitvec >> WRITE_REQUEST_BIT_OFFSET) & WRITE_REQUEST_BIT_MASK;
	}

	// write lock bit
	void set_write_lock_bit()
	{
		clear_write_lock_bit();
		bitvec |= WRITE_LOCK_BIT_MASK << WRITE_LOCK_BIT_OFFSET;
	}

	void clear_write_lock_bit()
	{
		bitvec &= ~(WRITE_LOCK_BIT_MASK << WRITE_LOCK_BIT_OFFSET);
	}

	bool get_write_lock_bit() const
	{
		return (bitvec >> WRITE_LOCK_BIT_OFFSET) & WRITE_LOCK_BIT_MASK;
	}

	// table id

	void set_table_id(uint64_t table_id)
	{
		DCHECK(table_id < (1 << 5));
		clear_table_id();
		bitvec |= table_id << TABLE_ID_OFFSET;
	}

	void clear_table_id()
	{
		bitvec &= ~(TABLE_ID_MASK << TABLE_ID_OFFSET);
	}

	uint64_t get_table_id() const
	{
		return (bitvec >> TABLE_ID_OFFSET) & TABLE_ID_MASK;
	}
	// partition id

	void set_partition_id(uint64_t partition_id)
	{
		DCHECK(partition_id < (1ULL << 32));
		clear_partition_id();
		bitvec |= partition_id << PARTITION_ID_OFFSET;
	}

	void clear_partition_id()
	{
		bitvec &= ~(PARTITION_ID_MASK << PARTITION_ID_OFFSET);
	}

	uint64_t get_partition_id() const
	{
		return (bitvec >> PARTITION_ID_OFFSET) & PARTITION_ID_MASK;
	}

	// tid
	uint64_t get_tid() const
	{
		return tid;
	}

	void set_tid(uint64_t tid)
	{
		this->tid = tid;
	}

	// key
	void set_key(const void *key)
	{
		this->key = key;
	}

	const void *get_key() const
	{
		return key;
	}

	// value
	void set_value(void *value)
	{
		this->value = value;
	}

	void *get_value() const
	{
		return value;
	}

	uint64_t get_rts() const
	{
		return rts;
	}

	void set_rts(uint64_t rts)
	{
		this->rts = rts;
	}

	uint64_t get_wts() const
	{
		return wts;
	}

	void set_wts(uint64_t wts)
	{
		this->wts = wts;
	}

	int get_read_set_pos()
	{
		return read_set_pos;
	}

	void set_read_set_pos(int32_t pos)
	{
		DCHECK(this->read_set_pos = -1);
		this->read_set_pos = pos;
	}

    private:
	/*
	 * A bitvec is a 32-bit word.
	 *
	 * [ table id (5) ] | partition id (8) | unused bit (16) |
	 * write lock bit(1) | read request bit (1) | local index read (1)  ]
	 *
	 * write lock bit is set when a write lock is acquired.
	 * read request bit is set when the read response is received.
	 * local index read  is set when the read is from a local read only index.
	 *
	 */

	uint64_t bitvec = 0;
	uint64_t tid = 0;
	const void *key = nullptr;
	void *value = nullptr;
	uint64_t rts = 0;
	uint64_t wts = 0;
	int32_t read_set_pos = -1;

    public:
	static constexpr uint64_t TABLE_ID_MASK = 0x1f;
	static constexpr uint64_t TABLE_ID_OFFSET = 27 + 24;

	static constexpr uint64_t PARTITION_ID_MASK = 0xffffffff;
	static constexpr uint64_t PARTITION_ID_OFFSET = 19;

	static constexpr uint64_t WRITE_LOCK_BIT_MASK = 0x1;
	static constexpr uint64_t WRITE_LOCK_BIT_OFFSET = 3;

	static constexpr uint64_t WRITE_REQUEST_BIT_MASK = 0x1;
	static constexpr uint64_t WRITE_REQUEST_BIT_OFFSET = 2;

	static constexpr uint64_t READ_REQUEST_BIT_MASK = 0x1;
	static constexpr uint64_t READ_REQUEST_BIT_OFFSET = 1;

	static constexpr uint64_t LOCAL_INDEX_READ_BIT_MASK = 0x1;
	static constexpr uint64_t LOCAL_INDEX_READ_BIT_OFFSET = 0;
};
} // namespace star

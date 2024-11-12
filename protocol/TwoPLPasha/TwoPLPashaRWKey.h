//
// Created by Xinjing on 9/12/21.
//

#pragma once

#include <algorithm>
#include <atomic>
#include <thread>

#include <glog/logging.h>

#include "core/Table.h"

namespace star
{

class TwoPLPashaRWKey {
    public:
        // range query types
        enum { SCAN_FOR_READ, SCAN_FOR_UPDATE, SCAN_FOR_INSERT, SCAN_FOR_DELETE };

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

	// read lock bit

	void set_read_lock_bit()
	{
		clear_read_lock_bit();
		bitvec |= READ_LOCK_BIT_MASK << READ_LOCK_BIT_OFFSET;
	}

	void clear_read_lock_bit()
	{
		bitvec &= ~(READ_LOCK_BIT_MASK << READ_LOCK_BIT_OFFSET);
	}

	uint64_t get_read_lock_bit() const
	{
		return (bitvec >> READ_LOCK_BIT_OFFSET) & READ_LOCK_BIT_MASK;
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

	uint64_t get_write_lock_bit() const
	{
		return (bitvec >> WRITE_LOCK_BIT_OFFSET) & WRITE_LOCK_BIT_MASK;
	}

	// read lock request bit

	void set_read_lock_request_bit()
	{
		clear_read_lock_request_bit();
		bitvec |= READ_LOCK_REQUEST_BIT_MASK << READ_LOCK_REQUEST_BIT_OFFSET;
	}

	void clear_read_lock_request_bit()
	{
		bitvec &= ~(READ_LOCK_REQUEST_BIT_MASK << READ_LOCK_REQUEST_BIT_OFFSET);
	}

	uint64_t get_read_lock_request_bit() const
	{
		return (bitvec >> READ_LOCK_REQUEST_BIT_OFFSET) & READ_LOCK_REQUEST_BIT_MASK;
	}

	// write lock request bit

	void set_write_lock_request_bit()
	{
		clear_write_lock_request_bit();
		bitvec |= WRITE_LOCK_REQUEST_BIT_MASK << WRITE_LOCK_REQUEST_BIT_OFFSET;
	}

	void clear_write_lock_request_bit()
	{
		bitvec &= ~(WRITE_LOCK_REQUEST_BIT_MASK << WRITE_LOCK_REQUEST_BIT_OFFSET);
	}

	uint64_t get_write_lock_request_bit() const
	{
		return (bitvec >> WRITE_LOCK_REQUEST_BIT_OFFSET) & WRITE_LOCK_REQUEST_BIT_MASK;
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
		DCHECK(partition_id < (1ULL << 20));
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

	// granule
	void set_granule_id(uint64_t granule_id)
	{
		DCHECK(granule_id < (1ULL << 17));
		clear_granule_id();
		bitvec |= granule_id << GRANULE_ID_OFFSET;
	}

	void clear_granule_id()
	{
		bitvec &= ~(GRANULE_ID_MASK << GRANULE_ID_OFFSET);
	}

	uint64_t get_granule_id() const
	{
		return (bitvec >> GRANULE_ID_OFFSET) & GRANULE_ID_MASK;
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

	void set_lock_index(std::uint32_t lock_index)
	{
		this->lock_index = lock_index;
	}

	int32_t get_lock_index()
	{
		return lock_index;
	}

        // scan
        const void *get_scan_min_key() const
	{
                return this->min_key;
	}

        const void *get_scan_max_key() const
	{
                return this->max_key;
	}

        uint64_t get_scan_limit() const
	{
                return this->limit;
	}

        void *get_scan_res_vec() const
	{
                return this->scan_results;
	}

        int get_request_type() const
	{
                return this->type;
	}

        void set_scan_args(const void *min_key, const void *max_key, uint64_t limit, void *results, int type)
	{
		this->min_key = min_key;
                this->max_key = max_key;
                this->limit = limit;
                this->scan_results = results;
                this->type = type;
	}

        // next-tuple
        ITable::row_entity get_next_row_entity() const
        {
                return this->next_row_entity;
        }

        void set_next_row_entity(ITable::row_entity &next_row_entity)
        {
                this->next_row_entity = next_row_entity;
        }

        bool get_next_row_locked() const
	{
                return this->is_next_row_locked;
	}

        void set_next_row_locked()
	{
                this->is_next_row_locked = true;
	}

        bool get_require_lock_next_row() const
	{
                return this->require_lock_next_row;
	}

        void set_require_lock_next_row()
	{
                this->require_lock_next_row = true;
	}

        // processed or not
        bool get_processed() const
	{
                return this->processed;
	}

        void set_processed()
	{
                this->processed = true;
	}

        // reference counting
        bool get_reference_counted()
	{
		return reference_counted;
	}

	void set_reference_counted()
	{
		DCHECK(this->reference_counted == false);
		this->reference_counted = true;
	}

        // remote insert
        char *get_inserted_cxl_row()
        {
                return this->inserted_cxl_row;
        }

        void set_inserted_cxl_row(char *cxl_row)
        {
                this->inserted_cxl_row = cxl_row;
        }

    private:
	/*
	 * A bitvec is a 64-bit word.
	 *
	 * [ table id (5) ] | partition id (32) | unused bit (14) |
	 *   write lock request bit (1) | read lock request bit (1)
	 *   write lock bit(1) | read lock bit (1) | local index read (1)  ]
	 *
	 *
	 * local index read  is set when the read is from a local read only index.
	 * write lock bit is set when a write lock is acquired.
	 * read lock bit is set when a read lock is acquired.
	 * write lock request bit is set when a write lock request is needed.
	 * read lock request bit is set when a read lock request is needed.
	 *
	 */
	const void *key = nullptr;
	void *value = nullptr;
	uint64_t bitvec = 0;
	uint64_t tid = 0;
	int32_t lock_index = -1;

        // for range scan
        const void *min_key = nullptr;
        const void *max_key = nullptr;
        uint64_t limit = 0;
        void *scan_results = nullptr;
        int type = 0;

        // next-tuple
        ITable::row_entity next_row_entity;
        bool require_lock_next_row = false;
        bool is_next_row_locked = false;

        // for move in & out
        bool reference_counted = false;

        bool processed = false;

        // for remote insert
        char *inserted_cxl_row = nullptr;

    public:
	static constexpr uint64_t TABLE_ID_MASK = 0x1f;
	static constexpr uint64_t TABLE_ID_OFFSET = 57;

	static constexpr uint64_t GRANULE_ID_MASK = 0x3ffff;
	static constexpr uint64_t GRANULE_ID_OFFSET = 39;

	static constexpr uint64_t PARTITION_ID_MASK = 0xfffff;
	static constexpr uint64_t PARTITION_ID_OFFSET = 19;

	static constexpr uint64_t WRITE_LOCK_REQUEST_BIT_MASK = 0x1;
	static constexpr uint64_t WRITE_LOCK_REQUEST_BIT_OFFSET = 4;

	static constexpr uint64_t READ_LOCK_REQUEST_BIT_MASK = 0x1;
	static constexpr uint64_t READ_LOCK_REQUEST_BIT_OFFSET = 3;

	static constexpr uint64_t WRITE_LOCK_BIT_MASK = 0x1;
	static constexpr uint64_t WRITE_LOCK_BIT_OFFSET = 2;

	static constexpr uint64_t READ_LOCK_BIT_MASK = 0x1;
	static constexpr uint64_t READ_LOCK_BIT_OFFSET = 1;

	static constexpr uint64_t LOCAL_INDEX_READ_BIT_MASK = 0x1;
	static constexpr uint64_t LOCAL_INDEX_READ_BIT_OFFSET = 0;
};
} // namespace star

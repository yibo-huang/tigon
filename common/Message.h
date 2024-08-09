//
// Created by Yi Lu on 8/28/18.
//

#pragma once

#include "StringPiece.h"
#include "common/MessagePiece.h"
#include "common/Time.h"
#include <chrono>
#include <string>
#include <functional>

namespace star
{

/*
 * Message header format
 *
 * | source node id (7=> 128) | dest node id (7=> 128) | worker id (8 => 256)
 * | count (15 => 2^15 = 32768) | length (27 => 2^27 = 134217728) |
 *
 * Note that, the header is included in the message size.
 *
 * Message format
 *
 * | Message header (64 bits) | 0xdeadbeef (32 bits) | (message pieces) * |
 *
 *
 * Message piece format
 *
 * | MessagePiece header (32 bits) | binary data |
 *
 * It's the user's responsibility to call flush().
 * For each message, flush() can only be called once which increments the count
 * of messages in message header and makes the size in message
 * header equal to the length of data.
 *
 */

class Message {
    public:
	// TODO: make it a C++ compatible forward iterator

	class Iterator {
	    public:
		Iterator(const char *ptr, const char *eof)
			: eof(eof)
			, messagePiece(get_message_piece(ptr))
		{
		}

		// Prefix ++ overload
		Iterator &operator++()
		{
			const char *ptr = messagePiece.stringPiece.data() + messagePiece.get_message_length();
			messagePiece = get_message_piece(ptr);
			return *this;
		}

		// Postfix ++ overload
		Iterator operator++(int)
		{
			Iterator iterator = *this;
			++(*this);
			return iterator;
		}

		bool operator==(const Iterator &that) const
		{
			return messagePiece == that.messagePiece && eof == that.eof;
		}

		bool operator!=(const Iterator &that) const
		{
			return !(*this == that);
		}

		MessagePiece &operator*()
		{
			return messagePiece;
		}

	    private:
		uint32_t get_message_length(const char *ptr)
		{
			return MessagePiece::get_message_length(*reinterpret_cast<const MessagePiece::header_type *>(ptr));
		}

		MessagePiece get_message_piece(const char *ptr)
		{
			DCHECK(ptr <= eof);
			if (ptr == eof) {
				return MessagePiece(StringPiece());
			}
			return MessagePiece(StringPiece(ptr, get_message_length(ptr)));
		}

	    private:
		const char *eof;
		MessagePiece messagePiece;
	};

	using header_type = uint64_t;
	using deadbeef_type = uint32_t;
	using source_cluster_worker_id_type = int32_t;
	using transaction_id_type = int64_t;
	using iterator_type = Iterator;

	Message()
		: data(get_prefix_size(), 0)
	{
		set_message_length(data.size());
		get_deadbeef_ref() = DEADBEEF;
		gen_time = Time::now();
		set_put_to_in_queue_time(gen_time);
		set_put_to_out_queue_time(gen_time);
	}

	virtual ~Message()
	{
	}

	void set_put_to_in_queue_time(uint64_t ts)
	{
		put_to_in_queue_time = ts;
	}
	uint64_t get_put_to_in_queue_time()
	{
		return put_to_in_queue_time;
	}
	void set_gen_time(uint64_t ts)
	{
		gen_time = ts;
	}
	uint64_t get_gen_time()
	{
		return gen_time;
	}
	void set_put_to_out_queue_time(uint64_t t)
	{
		put_to_out_queue_time = t;
	}
	uint64_t get_put_to_out_queue_time()
	{
		return put_to_out_queue_time;
	}
	std::chrono::steady_clock::time_point get_flush_time()
	{
		return time;
	}

	void resize(std::size_t size)
	{
		DCHECK(data.size() == get_prefix_size());
		data.resize(size);
		set_message_length(data.size());
		get_deadbeef_ref() = DEADBEEF;
	}

	virtual char *get_raw_ptr()
	{
		return &data[0];
	}

	void clear()
	{
		data = std::string(get_prefix_size(), 0);
		set_message_length(data.size());
		get_deadbeef_ref() = DEADBEEF;
	}

	void clear_message_pieces()
	{
		data.resize(get_prefix_size());
		set_message_length(data.size());
		set_message_count(0);
		get_deadbeef_ref() = DEADBEEF;
	}

	void flush()
	{
		auto message_count = get_message_count();
		set_message_count(message_count + 1);
		set_message_length(data.length());
		time = std::chrono::steady_clock::now();
	}

	bool check_size()
	{
		return get_message_length() == data.size();
	}

	bool check_deadbeef()
	{
		auto deadbeef = get_deadbeef_ref();
		return deadbeef == DEADBEEF;
	}

	Iterator begin()
	{
		auto eof = &data[0] + data.size();
		return Iterator(&data[0] + get_prefix_size(), eof);
	}

	Iterator end()
	{
		auto eof = &data[0] + data.size();
		return Iterator(eof, eof);
	}

    public:
	void set_is_replica(bool is_replica)
	{
		clear_source_node_id();
		get_header_ref() |= (is_replica << IS_REPLICA_OFFSET);
	}
	// Whether this message is designated to a replica
	uint64_t get_is_replica()
	{
		return (get_header_ref() >> IS_REPLICA_OFFSET) & IS_REPLICA_MASK;
	}

	void set_source_node_id(uint64_t source_node_id)
	{
		DCHECK(source_node_id < (1 << 7));
		clear_source_node_id();
		get_header_ref() |= (source_node_id << SOURCE_NODE_ID_OFFSET);
	}

	uint64_t get_source_node_id()
	{
		return (get_header_ref() >> SOURCE_NODE_ID_OFFSET) & SOURCE_NODE_ID_MASK;
	}

	virtual void set_dest_node_id(uint64_t dest_node_id)
	{
		DCHECK(dest_node_id < (1 << 7));
		clear_dest_node_id();
		get_header_ref() |= (dest_node_id << DEST_NODE_ID_OFFSET);
	}

	virtual uint64_t get_dest_node_id()
	{
		return (get_header_ref() >> DEST_NODE_ID_OFFSET) & DEST_NODE_ID_MASK;
	}

	void set_worker_id(uint64_t worker_id)
	{
		DCHECK(worker_id < (1 << 8));
		clear_worker_id();
		get_header_ref() |= (worker_id << WORKER_ID_OFFSET);
	}

	uint64_t get_worker_id()
	{
		return (get_header_ref() >> WORKER_ID_OFFSET) & WORKER_ID_MASK;
	}

	uint64_t get_message_count()
	{
		return (get_header_ref() >> MESSAGE_COUNT_OFFSET) & MESSAGE_COUNT_MASK;
	}

	virtual uint64_t get_message_length()
	{
		return (get_header_ref() >> MESSAGE_LENGTH_OFFSET) & MESSAGE_LENGTH_MASK;
	}

	int32_t get_source_cluster_worker_id()
	{
		return *reinterpret_cast<uint32_t *>(&data[0] + sizeof(header_type) + sizeof(deadbeef_type));
	}

	void set_source_cluster_worker_id(int32_t id)
	{
		*reinterpret_cast<uint32_t *>(&data[0] + sizeof(header_type) + sizeof(deadbeef_type)) = id;
	}

	uint64_t get_transaction_id() const
	{
		return *reinterpret_cast<const uint64_t *>(&data[0] + sizeof(header_type) + sizeof(deadbeef_type) + sizeof(source_cluster_worker_id_type));
	}

	void set_transaction_id(uint64_t tid)
	{
		*reinterpret_cast<uint64_t *>(&data[0] + sizeof(header_type) + sizeof(deadbeef_type) + sizeof(source_cluster_worker_id_type)) = tid;
	}

	size_t size_as_of_transaction_id() const
	{
		return sizeof(header_type) + sizeof(deadbeef_type) + sizeof(source_cluster_worker_id_type) + sizeof(transaction_id_type);
	}

	size_t size_as_of_messaeg_gen_time() const
	{
		return size_as_of_transaction_id() + sizeof(uint64_t);
	}

	size_t size_as_of_messaeg_send_time() const
	{
		return size_as_of_messaeg_gen_time() + sizeof(uint64_t);
	}

	size_t size_as_of_messaeg_recv_time() const
	{
		return size_as_of_messaeg_send_time() + sizeof(uint64_t);
	}

	size_t size_as_of_messaeg_resp_time() const
	{
		return size_as_of_messaeg_recv_time() + sizeof(uint64_t);
	}

	uint64_t get_message_gen_time() const
	{
		return *reinterpret_cast<const uint64_t *>(&data[0] + size_as_of_transaction_id());
	}

	uint64_t get_message_send_time() const
	{
		return *reinterpret_cast<const uint64_t *>(&data[0] + size_as_of_messaeg_gen_time());
	}

	uint64_t get_message_recv_time() const
	{
		return *reinterpret_cast<const uint64_t *>(&data[0] + size_as_of_messaeg_send_time());
	}

	uint64_t get_message_resp_time() const
	{
		return *reinterpret_cast<const uint64_t *>(&data[0] + size_as_of_messaeg_recv_time());
	}

	void set_message_gen_time(uint64_t t)
	{
		*reinterpret_cast<uint64_t *>(&data[0] + size_as_of_transaction_id()) = t;
	}

	void set_message_send_time(uint64_t t)
	{
		*reinterpret_cast<uint64_t *>(&data[0] + size_as_of_messaeg_gen_time()) = t;
	}

	void set_message_recv_time(uint64_t t)
	{
		*reinterpret_cast<uint64_t *>(&data[0] + size_as_of_messaeg_send_time()) = t;
	}

	void set_message_resp_time(uint64_t t)
	{
		*reinterpret_cast<uint64_t *>(&data[0] + size_as_of_messaeg_recv_time()) = t;
	}

    private:
	void clear_is_replica_bit()
	{
		get_header_ref() &= ~(IS_REPLICA_MASK << IS_REPLICA_OFFSET);
	}

	void clear_source_node_id()
	{
		get_header_ref() &= ~(SOURCE_NODE_ID_MASK << SOURCE_NODE_ID_OFFSET);
	}

	void clear_dest_node_id()
	{
		get_header_ref() &= ~(DEST_NODE_ID_MASK << DEST_NODE_ID_OFFSET);
	}

	void clear_worker_id()
	{
		get_header_ref() &= ~(WORKER_ID_MASK << WORKER_ID_OFFSET);
	}

	void clear_message_count()
	{
		get_header_ref() &= ~(MESSAGE_COUNT_MASK << MESSAGE_COUNT_OFFSET);
	}

	void clear_message_length()
	{
		get_header_ref() &= ~(MESSAGE_LENGTH_MASK << MESSAGE_LENGTH_OFFSET);
	}

	void set_message_count(uint64_t message_count)
	{
		DCHECK(message_count < (1 << 15));
		clear_message_count();
		get_header_ref() |= (message_count << MESSAGE_COUNT_OFFSET);
	}

	void set_message_length(uint64_t message_length)
	{
		DCHECK(message_length < (1 << 27));
		clear_message_length();
		get_header_ref() |= (message_length << MESSAGE_LENGTH_OFFSET);
	}

    private:
	uint64_t &get_header_ref()
	{
		return *reinterpret_cast<uint64_t *>(&data[0]);
	}

	uint32_t &get_deadbeef_ref()
	{
		return *reinterpret_cast<uint32_t *>(&data[0] + sizeof(header_type));
	}

    public:
	std::string data;
	std::chrono::steady_clock::time_point time;
	uint64_t gen_time;
	uint64_t put_to_in_queue_time;
	uint64_t put_to_out_queue_time;
	int ref_cnt = 0;

    public:
	static constexpr uint32_t get_prefix_size()
	{
		return sizeof(header_type) + sizeof(deadbeef_type) + sizeof(source_cluster_worker_id_type) + sizeof(transaction_id_type) + sizeof(uint64_t) * 4;
	}

	static uint64_t get_message_length(uint64_t v)
	{
		return (v >> MESSAGE_LENGTH_OFFSET) & MESSAGE_LENGTH_MASK;
	}

    public:
	static constexpr uint64_t SOURCE_NODE_ID_MASK = 0x7f;
	static constexpr uint64_t SOURCE_NODE_ID_OFFSET = 57;

	static constexpr uint64_t DEST_NODE_ID_MASK = 0x7f;
	static constexpr uint64_t DEST_NODE_ID_OFFSET = 50;

	static constexpr uint64_t WORKER_ID_MASK = 0xff;
	static constexpr uint64_t WORKER_ID_OFFSET = 42;

	static constexpr uint64_t MESSAGE_COUNT_MASK = 0x7fff;
	static constexpr uint64_t MESSAGE_COUNT_OFFSET = 27;

	static constexpr uint64_t MESSAGE_LENGTH_MASK = 0x3ffffffull;
	static constexpr uint64_t MESSAGE_LENGTH_OFFSET = 1;

	static constexpr uint64_t IS_REPLICA_MASK = 0x1l;
	static constexpr uint64_t IS_REPLICA_OFFSET = 0;

	static constexpr uint32_t DEADBEEF = 0xDEADBEEF;
};

class GrouppedMessage : public Message {
    public:
	GrouppedMessage()
	{
		clear();
	}
	GrouppedMessage(const std::string &group_data, uint64_t dest_node)
		: group_data(group_data)
		, dest_node(dest_node)
	{
	}
	void addMessage(Message *message)
	{
		group_data.append(message->get_raw_ptr(), message->get_message_length());
	}
	virtual char *get_raw_ptr() override
	{
		return &group_data[0];
	}
	virtual size_t get_message_length() override
	{
		return group_data.size();
	}
	virtual uint64_t get_dest_node_id() override
	{
		return dest_node;
	}
	virtual void set_dest_node_id(uint64_t dest_node) override
	{
		this->dest_node = dest_node;
	}
	void clear()
	{
		group_data.clear();
		dest_node = std::numeric_limits<uint64_t>::max();
	}

    private:
	std::string group_data;
	uint64_t dest_node;
};
} // namespace star
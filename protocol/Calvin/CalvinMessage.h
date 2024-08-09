//
// Created by Yi Lu on 9/13/18.
//

#pragma once

#include "common/Encoder.h"
#include "common/Message.h"
#include "common/MessagePiece.h"
#include "core/ControlMessage.h"
#include "core/Table.h"
#include "protocol/Calvin/CalvinRWKey.h"
#include "protocol/Calvin/CalvinTransaction.h"

namespace star
{

template <class Workload> class CalvinExecutor;

enum class CalvinMessage {
	READ_REQUEST = static_cast<int>(ControlMessage::NFIELDS),
	READ_RESPONSE,
	WRITE_REQUEST,
	WRITE_RESPONSE,
	LOCK_REQUEST,
	LOCK_RESPONSE,
	LOCK_REQUEST_DONE,
	NFIELDS
};

class CalvinMessageFactory {
    public:
	static std::size_t new_read_message(Message &message, ITable &table, std::size_t tid, uint32_t key_offset, const void *key)
	{
		/*
		 * The structure of a read request: (tid, key offset, key)
		 */

		auto key_size = table.key_size();
		auto message_size = MessagePiece::get_header_size() + sizeof(tid) + sizeof(key_offset) + key_size;

		auto message_piece_header = MessagePiece::construct_message_piece_header(static_cast<uint32_t>(CalvinMessage::READ_REQUEST), message_size,
											 table.tableID(), table.partitionID());

		Encoder encoder(message.data);
		encoder << message_piece_header;
		encoder << tid << key_offset;
		encoder.write_n_bytes(key, key_size);
		message.flush();
		return message_size;
	}

	static std::size_t new_write_message(Message &message, ITable &table, const void *key, const void *value)
	{
		/*
		 * The structure of a write request: (primary key, field value)
		 */

		auto key_size = table.key_size();
		auto value_size = table.field_size();

		auto message_size = MessagePiece::get_header_size() + key_size + value_size;

		auto message_piece_header = MessagePiece::construct_message_piece_header(static_cast<uint32_t>(CalvinMessage::WRITE_REQUEST), message_size,
											 table.tableID(), table.partitionID());

		Encoder encoder(message.data);
		encoder << message_piece_header;
		encoder.write_n_bytes(key, key_size);
		table.serialize_value(encoder, value);
		message.flush();
		return message_size;
	}
};

class CalvinMessageHandler {
	using Transaction = CalvinTransaction;

    public:
	template <class Workload>
	static void lock_request_handler(Message &inputMessage, MessagePiece inputPiece, Message &responseMessage, ITable &table, Transaction *txn,
					 CalvinExecutor<Workload> *executor)
	{
		DCHECK(inputPiece.get_message_type() == static_cast<uint32_t>(CalvinMessage::LOCK_REQUEST));
		/*
		 * The structure of a lock request: (tid, source_node, n_keys,
		  [table_id, partition_id, read_or_write, key_size, key_bytes] * n_keys)
		 */

		int64_t tid;
		std::size_t n_keys;
		int source_node_id;

		CalvinTransaction::TransactionLockRequest req;
		StringPiece stringPiece = inputPiece.toStringPiece();
		Decoder dec(stringPiece);
		dec >> req.tid >> req.source_coordinator >> n_keys;

		for (size_t i = 0; i < n_keys; ++i) {
			uint32_t table_id;
			uint32_t partition_id;
			std::size_t key_size;
			bool read_or_write;
			dec >> table_id >> partition_id >> read_or_write >> key_size;
			char *key_buffer = new char[key_size];
			dec.read_n_bytes(key_buffer, key_size);
			CalvinRWKey readKey;

			readKey.set_table_id(table_id);
			readKey.set_partition_id(partition_id);

			readKey.set_key(key_buffer);
			readKey.set_value(nullptr);

			if (read_or_write == false)
				readKey.set_read_lock_bit();
			else
				readKey.set_write_lock_bit();
			auto t = executor->db.find_table(table_id, partition_id);
			DCHECK(t->key_size() == key_size);
			req.add_key(readKey);
		}
		// LOG(INFO) << " worker " <<  executor->id << " received a lock request of " << req.keys.size() << " keys from source coordinator " <<
		// req.source_coordinator << " tid " << req.tid;
		executor->lock_requests_current_batch.push_back(req);
	}

	template <class Workload>
	static void lock_response_handler(Message &inputMessage, MessagePiece inputPiece, Message &responseMessage, ITable &table, Transaction *txn,
					  CalvinExecutor<Workload> *executor)
	{
		DCHECK(inputPiece.get_message_type() == static_cast<uint32_t>(CalvinMessage::LOCK_RESPONSE));
		/*
		 * The structure of a lock response: (tid, succees)
		 */

		int64_t tid;
		bool success;

		DCHECK(inputPiece.get_message_length() == MessagePiece::get_header_size() + sizeof(tid) + sizeof(success));

		StringPiece stringPiece = inputPiece.toStringPiece();
		Decoder dec(stringPiece);
		dec >> tid >> success;

		executor->collect_vote_for_txn(tid, success);
		executor->received_net_lock_responses++;
	}

	template <class Workload>
	static void read_request_handler(Message &inputMessage, MessagePiece inputPiece, Message &responseMessage, ITable &table, Transaction *txn,
					 CalvinExecutor<Workload> *executor)
	{
		DCHECK(inputPiece.get_message_type() == static_cast<uint32_t>(CalvinMessage::READ_REQUEST));
		auto table_id = inputPiece.get_table_id();
		auto partition_id = inputPiece.get_partition_id();
		DCHECK(table_id == table.tableID());
		DCHECK(partition_id == table.partitionID());
		auto key_size = table.key_size();
		auto value_size = table.field_size();

		/*
		 * The structure of a read request: (tid, key offset, key)
		 * The structure of a read response: (tid, key offset, value_size, value)
		 */

		std::size_t tid;
		uint32_t key_offset;

		auto input_piece_size = inputPiece.get_message_length();
		DCHECK(input_piece_size == MessagePiece::get_header_size() + sizeof(tid) + sizeof(key_offset) + key_size);

		StringPiece stringPiece = inputPiece.toStringPiece();
		Decoder dec(stringPiece);
		dec >> tid >> key_offset;

		const void *key = dec.bytes.data();
		auto row = table.search(key);

		std::atomic<uint64_t> &meta = *std::get<0>(row);

		//    DCHECK(CalvinHelper::is_read_locked(meta) || CalvinHelper::is_write_locked(meta));

		bool success = true;

		// prepare response message header
		auto message_size = MessagePiece::get_header_size() + sizeof(bool) + sizeof(key_offset) + sizeof(value_size);

		if (success) {
			message_size += value_size;
		}

		auto message_piece_header =
			MessagePiece::construct_message_piece_header(static_cast<uint32_t>(CalvinMessage::READ_RESPONSE), message_size, table_id, partition_id);

		star::Encoder encoder(responseMessage.data);
		encoder << message_piece_header;
		encoder << success << key_offset << value_size;

		if (success) {
			// reserve size for read
			responseMessage.data.append(value_size, 0);
			void *dest = &responseMessage.data[0] + responseMessage.data.size() - value_size;
			// read to message buffer
			CalvinHelper::read(row, dest, value_size);
		}
		responseMessage.set_transaction_id(inputMessage.get_transaction_id());
		responseMessage.flush();
		responseMessage.set_gen_time(Time::now());
	}

	template <class Workload>
	static void read_response_handler(Message &inputMessage, MessagePiece inputPiece, Message &responseMessage, ITable &table, Transaction *txn,
					  CalvinExecutor<Workload> *executor)
	{
		DCHECK(inputPiece.get_message_type() == static_cast<uint32_t>(CalvinMessage::READ_RESPONSE));
		auto table_id = inputPiece.get_table_id();
		auto partition_id = inputPiece.get_partition_id();
		DCHECK(table_id == table.tableID());
		DCHECK(partition_id == table.partitionID());
		auto key_size = table.key_size();
		auto value_size = table.field_size();

		/*
		 * The structure of a read lock response: (success?, key offset, value?,
		 * tid?)
		 */

		bool success;
		uint32_t key_offset;
		uint64_t v_size;

		StringPiece stringPiece = inputPiece.toStringPiece();
		Decoder dec(stringPiece);
		dec >> success >> key_offset >> v_size;
		DCHECK(v_size == value_size);

		if (success) {
			DCHECK(inputPiece.get_message_length() ==
			       MessagePiece::get_header_size() + sizeof(success) + sizeof(key_offset) + sizeof(value_size) + value_size);

			CalvinRWKey &readKey = txn->readSet[key_offset];
			dec.read_n_bytes(readKey.get_value(), value_size);
			readKey.set_read_lock_bit();
		}
		txn->remote_read--;
		txn->network_size += inputPiece.get_message_length();
	}

	template <class Workload>
	static void lock_request_done_handler(Message &inputMessage, MessagePiece inputPiece, Message &responseMessage, ITable &table, Transaction *txn,
					      CalvinExecutor<Workload> *executor)
	{
		DCHECK(inputPiece.get_message_type() == static_cast<uint32_t>(CalvinMessage::LOCK_REQUEST_DONE));

		/*
		 * The structure of a read request: (tid, key offset, key)
		 * The structure of a read response: (tid, key offset, value_size, value)
		 */

		int source_coordinator;

		auto input_piece_size = inputPiece.get_message_length();
		DCHECK(input_piece_size == MessagePiece::get_header_size() + sizeof(source_coordinator));

		StringPiece stringPiece = inputPiece.toStringPiece();
		Decoder dec(stringPiece);
		dec >> source_coordinator;

		executor->lock_request_done_received++;
	}

	template <class Workload>
	static void write_request_handler(Message &inputMessage, MessagePiece inputPiece, Message &responseMessage, ITable &table, Transaction *txn,
					  CalvinExecutor<Workload> *executor)
	{
		DCHECK(inputPiece.get_message_type() == static_cast<uint32_t>(CalvinMessage::WRITE_REQUEST));
		auto table_id = inputPiece.get_table_id();
		auto partition_id = inputPiece.get_partition_id();
		DCHECK(table_id == table.tableID());
		DCHECK(partition_id == table.partitionID());
		auto key_size = table.key_size();
		auto field_size = table.field_size();

		/*
		 * The structure of a write request: (primary key, field value)
		 * The structure of a write response: ()
		 */

		DCHECK(inputPiece.get_message_length() == MessagePiece::get_header_size() + key_size + field_size);

		Decoder dec(inputPiece.toStringPiece());
		auto stringPiece = dec.bytes;

		const void *key = stringPiece.data();
		stringPiece.remove_prefix(key_size);
		const void *value = stringPiece.data();
		table.deserialize_value(key, stringPiece);

		// prepare response message header
		auto message_size = MessagePiece::get_header_size();
		auto message_piece_header = MessagePiece::construct_message_piece_header(static_cast<uint32_t>(CalvinMessage::WRITE_RESPONSE), message_size,
											 table_id, partition_id);

		star::Encoder encoder(responseMessage.data);
		encoder << message_piece_header;
		responseMessage.set_transaction_id(inputMessage.get_transaction_id());
		responseMessage.flush();
		responseMessage.set_gen_time(Time::now());
	}

	template <class Workload>
	static void write_response_handler(Message &inputMessage, MessagePiece inputPiece, Message &responseMessage, ITable &table, Transaction *txn,
					   CalvinExecutor<Workload> *executor)
	{
		DCHECK(inputPiece.get_message_type() == static_cast<uint32_t>(CalvinMessage::WRITE_RESPONSE));
		auto table_id = inputPiece.get_table_id();
		auto partition_id = inputPiece.get_partition_id();
		DCHECK(table_id == table.tableID());
		DCHECK(partition_id == table.partitionID());
		auto key_size = table.key_size();

		/*
		 * The structure of a write response: ()
		 */

		txn->remote_write--;
		txn->network_size += inputPiece.get_message_length();
	}

	template <class Workload>
	static std::vector<std::function<void(Message &, MessagePiece, Message &, ITable &, Transaction *, CalvinExecutor<Workload> *)> > get_message_handlers()
	{
		std::vector<std::function<void(Message &, MessagePiece, Message &, ITable &, Transaction *, CalvinExecutor<Workload> *)> > v;
		v.resize(static_cast<int>(ControlMessage::NFIELDS));
		v.push_back(read_request_handler<Workload>);
		v.push_back(read_response_handler<Workload>);
		v.push_back(write_request_handler<Workload>);
		v.push_back(write_response_handler<Workload>);
		v.push_back(lock_request_handler<Workload>);
		v.push_back(lock_response_handler<Workload>);
		v.push_back(lock_request_done_handler<Workload>);
		return v;
	}
};

} // namespace star
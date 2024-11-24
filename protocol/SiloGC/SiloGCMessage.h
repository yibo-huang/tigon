//
// Created by Yi Lu on 9/10/18.
//

#pragma once

#include "common/Encoder.h"
#include "common/Message.h"
#include "common/MessagePiece.h"
#include "core/ControlMessage.h"
#include "core/Table.h"
#include "protocol/Silo/SiloHelper.h"
#include "protocol/Silo/SiloRWKey.h"
#include "protocol/Silo/SiloTransaction.h"

namespace star
{

enum class SiloGCMessage {
	SEARCH_REQUEST = static_cast<int>(ControlMessage::NFIELDS),
	SEARCH_RESPONSE,
	LOCK_REQUEST,
	LOCK_RESPONSE,
	READ_VALIDATION_REQUEST,
	READ_VALIDATION_RESPONSE,
	ABORT_REQUEST,
	WRITE_REQUEST,
	REPLICATION_REQUEST,
	NFIELDS
};

class SiloGCMessageFactory {
    public:
	static std::size_t new_search_message(Message &message, ITable &table, const void *key, uint32_t key_offset)
	{
		/*
		 * The structure of a search request: (primary key, read key offset)
		 */

		auto key_size = table.key_size();

		auto message_size = MessagePiece::get_header_size() + key_size + sizeof(key_offset);
		auto message_piece_header = MessagePiece::construct_message_piece_header(static_cast<uint32_t>(SiloGCMessage::SEARCH_REQUEST), message_size,
											 table.tableID(), table.partitionID());

		Encoder encoder(message.data);
		encoder << message_piece_header;
		encoder.write_n_bytes(key, key_size);
		encoder << key_offset;
		message.flush();
		return message_size;
	}

	static std::size_t new_lock_message(Message &message, ITable &table, const void *key, uint32_t key_offset)
	{
		/*
		 * The structure of a lock request: (primary key, write key offset)
		 */

		auto key_size = table.key_size();

		auto message_size = MessagePiece::get_header_size() + key_size + sizeof(key_offset);
		auto message_piece_header = MessagePiece::construct_message_piece_header(static_cast<uint32_t>(SiloGCMessage::LOCK_REQUEST), message_size,
											 table.tableID(), table.partitionID());

		Encoder encoder(message.data);
		encoder << message_piece_header;
		encoder.write_n_bytes(key, key_size);
		encoder << key_offset;
		message.flush();
		return message_size;
	}

	static std::size_t new_read_validation_message(Message &message, ITable &table, const void *key, uint32_t key_offset, uint64_t tid,
						       bool last_validation)
	{
		/*
		 * The structure of a read validation request: (primary key, read key
		 * offset, tid)
		 */

		auto key_size = table.key_size();

		auto message_size = MessagePiece::get_header_size() + key_size + sizeof(key_offset) + sizeof(tid) + sizeof(last_validation);
		auto message_piece_header = MessagePiece::construct_message_piece_header(static_cast<uint32_t>(SiloGCMessage::READ_VALIDATION_REQUEST),
											 message_size, table.tableID(), table.partitionID());

		Encoder encoder(message.data);
		encoder << message_piece_header;
		encoder.write_n_bytes(key, key_size);
		encoder << key_offset << tid << last_validation;
		message.flush();
		return message_size;
	}

	static std::size_t new_abort_message(Message &message, ITable &table, const void *key)
	{
		/*
		 * The structure of an abort request: (primary key)
		 */

		auto key_size = table.key_size();

		auto message_size = MessagePiece::get_header_size() + key_size;
		auto message_piece_header = MessagePiece::construct_message_piece_header(static_cast<uint32_t>(SiloGCMessage::ABORT_REQUEST), message_size,
											 table.tableID(), table.partitionID());

		Encoder encoder(message.data);
		encoder << message_piece_header;
		encoder.write_n_bytes(key, key_size);
		message.flush();
		return message_size;
	}

	static std::size_t new_write_message(Message &message, ITable &table, const void *key, const void *value, uint64_t commit_tid)
	{
		/*
		 * The structure of a write request: (primary key, field value, commit_tid)
		 */

		auto key_size = table.key_size();
		auto field_size = table.field_size();

		auto message_size = MessagePiece::get_header_size() + key_size + field_size + sizeof(commit_tid);
		auto message_piece_header = MessagePiece::construct_message_piece_header(static_cast<uint32_t>(SiloGCMessage::WRITE_REQUEST), message_size,
											 table.tableID(), table.partitionID());

		Encoder encoder(message.data);
		encoder << message_piece_header;
		encoder.write_n_bytes(key, key_size);
		table.serialize_value(encoder, value);
		encoder << commit_tid;
		message.flush();
		return message_size;
	}

	static std::size_t new_replication_message(Message &message, ITable &table, const void *key, const void *value, uint64_t commit_tid)
	{
		/*
		 * The structure of a replication request: (primary key, field value,
		 * commit_tid)
		 */

		auto key_size = table.key_size();
		auto field_size = table.field_size();

		auto message_size = MessagePiece::get_header_size() + key_size + field_size + sizeof(commit_tid);
		auto message_piece_header = MessagePiece::construct_message_piece_header(static_cast<uint32_t>(SiloGCMessage::REPLICATION_REQUEST),
											 message_size, table.tableID(), table.partitionID());

		Encoder encoder(message.data);
		encoder << message_piece_header;
		encoder.write_n_bytes(key, key_size);
		table.serialize_value(encoder, value);
		encoder << commit_tid;
		message.flush();
		return message_size;
	}
};

class SiloGCMessageHandler {
	using Transaction = SiloTransaction;

    public:
	static void search_request_handler(MessagePiece inputPiece, Message &responseMessage, ITable &table, Transaction *txn)
	{
		DCHECK(inputPiece.get_message_type() == static_cast<uint32_t>(SiloGCMessage::SEARCH_REQUEST));
		auto table_id = inputPiece.get_table_id();
		auto partition_id = inputPiece.get_partition_id();
		DCHECK(table_id == table.tableID());
		DCHECK(partition_id == table.partitionID());
		auto key_size = table.key_size();
		auto value_size = table.value_size();

		/*
		 * The structure of a read request: (primary key, read key offset)
		 * The structure of a read response: (value, tid, read key offset)
		 */

		auto stringPiece = inputPiece.toStringPiece();
		uint32_t key_offset;

		DCHECK(inputPiece.get_message_length() == MessagePiece::get_header_size() + key_size + sizeof(key_offset));

		// get row and offset
		const void *key = stringPiece.data();
		auto row = table.search(key);

		stringPiece.remove_prefix(key_size);
		star::Decoder dec(stringPiece);
		dec >> key_offset;

		DCHECK(dec.size() == 0);

		// prepare response message header
		auto message_size = MessagePiece::get_header_size() + value_size + sizeof(uint64_t) + sizeof(key_offset);
		auto message_piece_header = MessagePiece::construct_message_piece_header(static_cast<uint32_t>(SiloGCMessage::SEARCH_RESPONSE), message_size,
											 table_id, partition_id);

		star::Encoder encoder(responseMessage.data);
		encoder << message_piece_header;

		// reserve size for read
		responseMessage.data.append(value_size, 0);
		void *dest = &responseMessage.data[0] + responseMessage.data.size() - value_size;
		// read to message buffer
		auto tid = SiloHelper::read(row, dest, value_size);

		encoder << tid << key_offset;
		responseMessage.flush();
	}

	static void search_response_handler(MessagePiece inputPiece, Message &responseMessage, ITable &table, Transaction *txn)
	{
		DCHECK(inputPiece.get_message_type() == static_cast<uint32_t>(SiloGCMessage::SEARCH_RESPONSE));
		auto table_id = inputPiece.get_table_id();
		auto partition_id = inputPiece.get_partition_id();
		DCHECK(table_id == table.tableID());
		DCHECK(partition_id == table.partitionID());
		auto key_size = table.key_size();
		auto value_size = table.value_size();

		/*
		 * The structure of a read response: (value, tid, read key offset)
		 */

		uint64_t tid;
		uint32_t key_offset;

		DCHECK(inputPiece.get_message_length() == MessagePiece::get_header_size() + value_size + sizeof(tid) + sizeof(key_offset));

		StringPiece stringPiece = inputPiece.toStringPiece();
		stringPiece.remove_prefix(value_size);
		Decoder dec(stringPiece);
		dec >> tid >> key_offset;

		SiloRWKey &readKey = txn->readSet[key_offset];
		dec = Decoder(inputPiece.toStringPiece());
		dec.read_n_bytes(readKey.get_value(), value_size);
		readKey.set_tid(tid);
		txn->pendingResponses--;
		txn->network_size += inputPiece.get_message_length();
	}

	static void lock_request_handler(MessagePiece inputPiece, Message &responseMessage, ITable &table, Transaction *txn)
	{
		DCHECK(inputPiece.get_message_type() == static_cast<uint32_t>(SiloGCMessage::LOCK_REQUEST));
		auto table_id = inputPiece.get_table_id();
		auto partition_id = inputPiece.get_partition_id();
		DCHECK(table_id == table.tableID());
		DCHECK(partition_id == table.partitionID());
		auto key_size = table.key_size();

		/*
		 * The structure of a lock request: (primary key, write key offset)
		 * The structure of a lock response: (success?, tid, write key offset)
		 */

		auto stringPiece = inputPiece.toStringPiece();

		uint32_t key_offset;

		DCHECK(inputPiece.get_message_length() == MessagePiece::get_header_size() + key_size + sizeof(key_offset));

		const void *key = stringPiece.data();
		std::atomic<uint64_t> &tid = *table.search_metadata(key);

		bool success;
		uint64_t latest_tid = SiloHelper::lock(tid, success);

		stringPiece.remove_prefix(key_size);
		star::Decoder dec(stringPiece);
		dec >> key_offset;

		DCHECK(dec.size() == 0);

		// prepare response message header
		auto message_size = MessagePiece::get_header_size() + sizeof(bool) + sizeof(uint64_t) + sizeof(uint32_t);
		auto message_piece_header =
			MessagePiece::construct_message_piece_header(static_cast<uint32_t>(SiloGCMessage::LOCK_RESPONSE), message_size, table_id, partition_id);

		star::Encoder encoder(responseMessage.data);
		encoder << message_piece_header;
		encoder << success << latest_tid << key_offset;
		responseMessage.flush();
	}

	static void lock_response_handler(MessagePiece inputPiece, Message &responseMessage, ITable &table, Transaction *txn)
	{
		DCHECK(inputPiece.get_message_type() == static_cast<uint32_t>(SiloGCMessage::LOCK_RESPONSE));
		auto table_id = inputPiece.get_table_id();
		auto partition_id = inputPiece.get_partition_id();
		DCHECK(table_id == table.tableID());
		DCHECK(partition_id == table.partitionID());
		auto key_size = table.key_size();

		/*
		 * The structure of a lock response: (success?, tid, write key offset)
		 */

		bool success;
		uint64_t latest_tid;
		uint32_t key_offset;

		DCHECK(inputPiece.get_message_length() == MessagePiece::get_header_size() + sizeof(success) + sizeof(latest_tid) + sizeof(key_offset));

		StringPiece stringPiece = inputPiece.toStringPiece();
		Decoder dec(stringPiece);
		dec >> success >> latest_tid >> key_offset;

		DCHECK(dec.size() == 0);

		SiloRWKey &writeKey = txn->writeSet[key_offset];

		bool tid_changed = false;

		if (success) {
			SiloRWKey *readKey = txn->get_read_key(writeKey.get_key());

			DCHECK(readKey != nullptr);

			uint64_t tid_on_read = readKey->get_tid();

			if (latest_tid != tid_on_read) {
				tid_changed = true;
			}

			writeKey.set_tid(latest_tid);
			writeKey.set_write_lock_bit();
		}

		txn->pendingResponses--;
		txn->network_size += inputPiece.get_message_length();

		if (!success || tid_changed) {
			txn->abort_lock = true;
		}
	}

	static void read_validation_request_handler(MessagePiece inputPiece, Message &responseMessage, ITable &table, Transaction *txn)
	{
		DCHECK(inputPiece.get_message_type() == static_cast<uint32_t>(SiloGCMessage::READ_VALIDATION_REQUEST));
		auto table_id = inputPiece.get_table_id();
		auto partition_id = inputPiece.get_partition_id();
		DCHECK(table_id == table.tableID());
		DCHECK(partition_id == table.partitionID());
		auto key_size = table.key_size();

		/*
		 * The structure of a read validation request: (primary key, read key
		 * offset, tid, last_validation) The structure of a read validation response: (success?, read
		 * key offset)
		 */

		DCHECK(inputPiece.get_message_length() == MessagePiece::get_header_size() + key_size + sizeof(uint32_t) + sizeof(uint64_t) + sizeof(bool));

		auto stringPiece = inputPiece.toStringPiece();
		const void *key = stringPiece.data();
		auto latest_tid = table.search_metadata(key)->load();
		stringPiece.remove_prefix(key_size);

		uint32_t key_offset;
		uint64_t tid;
		bool last_validation;
		Decoder dec(stringPiece);
		dec >> key_offset >> tid >> last_validation;

		bool success = true;

		if (SiloHelper::remove_lock_bit(latest_tid) != tid) {
			success = false;
		}

		if (SiloHelper::is_locked(latest_tid)) { // must be locked by others
			success = false;
		}

		// prepare response message header
		auto message_size = MessagePiece::get_header_size() + sizeof(bool) + sizeof(uint32_t);
		auto message_piece_header = MessagePiece::construct_message_piece_header(static_cast<uint32_t>(SiloGCMessage::READ_VALIDATION_RESPONSE),
											 message_size, table_id, partition_id);

		star::Encoder encoder(responseMessage.data);
		encoder << message_piece_header;
		encoder << success << key_offset;

		responseMessage.flush();

		// std::size_t lsn = 0;
		if (txn->get_logger()) {
			// write a vote for a key
			std::ostringstream ss;
			ss << success;
			auto output = ss.str();
			txn->get_logger()->write(output.c_str(), output.size(), last_validation, txn->startTime, [&]() { txn->remote_request_handler(0); });
		}

		if (txn->get_logger() && last_validation) {
			// sync the votes
			// On recovery, the txn is considered prepared only if all votes are true // passed all validation
			// txn->get_logger()->sync(lsn, [&](){ txn->remote_request_handler(0); });
		}
	}

	static void read_validation_response_handler(MessagePiece inputPiece, Message &responseMessage, ITable &table, Transaction *txn)
	{
		DCHECK(inputPiece.get_message_type() == static_cast<uint32_t>(SiloGCMessage::READ_VALIDATION_RESPONSE));
		auto table_id = inputPiece.get_table_id();
		auto partition_id = inputPiece.get_partition_id();
		DCHECK(table_id == table.tableID());
		DCHECK(partition_id == table.partitionID());
		auto key_size = table.key_size();

		/*
		 * The structure of a read validation response: (success?, read key offset)
		 */

		bool success;
		uint32_t key_offset;

		Decoder dec(inputPiece.toStringPiece());

		dec >> success >> key_offset;

		SiloRWKey &readKey = txn->readSet[key_offset];

		txn->pendingResponses--;
		txn->network_size += inputPiece.get_message_length();

		if (!success) {
			txn->abort_read_validation = true;
		}
	}

	static void abort_request_handler(MessagePiece inputPiece, Message &responseMessage, ITable &table, Transaction *txn)
	{
		DCHECK(inputPiece.get_message_type() == static_cast<uint32_t>(SiloGCMessage::ABORT_REQUEST));
		auto table_id = inputPiece.get_table_id();
		auto partition_id = inputPiece.get_partition_id();
		DCHECK(table_id == table.tableID());
		DCHECK(partition_id == table.partitionID());
		auto key_size = table.key_size();

		/*
		 * The structure of an abort request: (primary key)
		 * The structure of an abort response: null
		 */

		DCHECK(inputPiece.get_message_length() == MessagePiece::get_header_size() + key_size);

		auto stringPiece = inputPiece.toStringPiece();
		const void *key = stringPiece.data();
		std::atomic<uint64_t> &tid = *table.search_metadata(key);

		// unlock the key
		SiloHelper::unlock(tid);
	}

	static void write_request_handler(MessagePiece inputPiece, Message &responseMessage, ITable &table, Transaction *txn)
	{
		DCHECK(inputPiece.get_message_type() == static_cast<uint32_t>(SiloGCMessage::WRITE_REQUEST));
		auto table_id = inputPiece.get_table_id();
		auto partition_id = inputPiece.get_partition_id();
		DCHECK(table_id == table.tableID());
		DCHECK(partition_id == table.partitionID());
		auto key_size = table.key_size();
		auto field_size = table.field_size();

		/*
		 * The structure of a write request: (primary key, field value, commit_tid)
		 * The structure of a write response: null
		 */

		DCHECK(inputPiece.get_message_length() == MessagePiece::get_header_size() + key_size + field_size + sizeof(uint64_t));

		auto stringPiece = inputPiece.toStringPiece();

		const void *key = stringPiece.data();
		stringPiece.remove_prefix(key_size);
		auto valueStringPiece = stringPiece;
		stringPiece.remove_prefix(field_size);

		uint64_t commit_tid;
		Decoder dec(stringPiece);
		dec >> commit_tid;

		DCHECK(dec.size() == 0);

		std::atomic<uint64_t> &tid = *table.search_metadata(key);
		table.deserialize_value(key, valueStringPiece);

		SiloHelper::unlock(tid, commit_tid);
	}

	static void replication_request_handler(MessagePiece inputPiece, Message &responseMessage, ITable &table, Transaction *txn)
	{
		DCHECK(inputPiece.get_message_type() == static_cast<uint32_t>(SiloGCMessage::REPLICATION_REQUEST));
		auto table_id = inputPiece.get_table_id();
		auto partition_id = inputPiece.get_partition_id();
		DCHECK(table_id == table.tableID());
		DCHECK(partition_id == table.partitionID());
		auto key_size = table.key_size();
		auto field_size = table.field_size();

		/*
		 * The structure of a replication request: (primary key, field value,
		 * commit_tid) The structure of a replication response: null
		 */

		DCHECK(inputPiece.get_message_length() == MessagePiece::get_header_size() + key_size + field_size + sizeof(uint64_t));

		auto stringPiece = inputPiece.toStringPiece();

		const void *key = stringPiece.data();
		stringPiece.remove_prefix(key_size);
		auto valueStringPiece = stringPiece;
		stringPiece.remove_prefix(field_size);

		uint64_t commit_tid;
		Decoder dec(stringPiece);
		dec >> commit_tid;

		DCHECK(dec.size() == 0);

		std::atomic<uint64_t> &tid = *table.search_metadata(key);

		uint64_t last_tid = SiloHelper::lock(tid);

		if (commit_tid > last_tid) {
			table.deserialize_value(key, valueStringPiece);
			SiloHelper::unlock(tid, commit_tid);
		} else {
			SiloHelper::unlock(tid);
		}
	}

	static std::vector<std::function<void(MessagePiece, Message &, ITable &, Transaction *)> > get_message_handlers()
	{
		std::vector<std::function<void(MessagePiece, Message &, ITable &, Transaction *)> > v;
		v.resize(static_cast<int>(ControlMessage::NFIELDS));
		v.push_back(SiloGCMessageHandler::search_request_handler);
		v.push_back(SiloGCMessageHandler::search_response_handler);
		v.push_back(SiloGCMessageHandler::lock_request_handler);
		v.push_back(SiloGCMessageHandler::lock_response_handler);
		v.push_back(SiloGCMessageHandler::read_validation_request_handler);
		v.push_back(SiloGCMessageHandler::read_validation_response_handler);
		v.push_back(SiloGCMessageHandler::abort_request_handler);
		v.push_back(SiloGCMessageHandler::write_request_handler);
		v.push_back(SiloGCMessageHandler::replication_request_handler);
		return v;
	}
};

} // namespace star

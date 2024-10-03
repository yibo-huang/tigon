//
// Created by Yi Lu on 9/6/18.
//

#pragma once

#include "common/Encoder.h"
#include "common/Message.h"
#include "common/MessagePiece.h"
#include "common/Operation.h"
#include "core/ControlMessage.h"
#include "core/Table.h"
#include "protocol/Silo/SiloHelper.h"
#include "protocol/Silo/SiloTransaction.h"

namespace star
{

enum class StarMessage {
	ASYNC_VALUE_REPLICATION_REQUEST = static_cast<int>(ControlMessage::NFIELDS),
	SYNC_VALUE_REPLICATION_REQUEST,
	SYNC_VALUE_REPLICATION_RESPONSE,
	OPERATION_REPLICATION_REQUEST,
	NFIELDS
};

class StarMessageFactory {
    public:
	static std::size_t new_async_value_replication_message(Message &message, ITable &table, const void *key, const void *value, uint64_t commit_tid)
	{
		/*
		 * The structure of an async value replication request: (primary key, field
		 * value, commit_tid)
		 */

		auto key_size = table.key_size();
		auto field_size = table.field_size();

		auto message_size = MessagePiece::get_header_size() + key_size + field_size + sizeof(commit_tid);
		auto message_piece_header = MessagePiece::construct_message_piece_header(static_cast<uint32_t>(StarMessage::ASYNC_VALUE_REPLICATION_REQUEST),
											 message_size, table.tableID(), table.partitionID());

		Encoder encoder(message.data);
		encoder << message_piece_header;
		encoder.write_n_bytes(key, key_size);
		table.serialize_value(encoder, value);
		encoder << commit_tid;
		message.flush();
		return message_size;
	}

	static std::size_t new_sync_value_replication_message(Message &message, ITable &table, const void *key, const void *value, uint64_t commit_tid)
	{
		/*
		 * The structure of a sync value replication request: (primary key, field
		 * value, commit_tid)
		 */

		auto key_size = table.key_size();
		auto field_size = table.field_size();

		auto message_size = MessagePiece::get_header_size() + key_size + field_size + sizeof(commit_tid);
		auto message_piece_header = MessagePiece::construct_message_piece_header(static_cast<uint32_t>(StarMessage::SYNC_VALUE_REPLICATION_REQUEST),
											 message_size, table.tableID(), table.partitionID());

		Encoder encoder(message.data);
		encoder << message_piece_header;
		encoder.write_n_bytes(key, key_size);
		table.serialize_value(encoder, value);
		encoder << commit_tid;
		message.flush();
		return message_size;
	}

	static std::size_t new_operation_replication_message(Message &message, const Operation &operation)
	{
		/*
		 * The structure of an operation replication message: (tid, data)
		 */

		auto message_size = MessagePiece::get_header_size() + sizeof(uint64_t) + operation.data.size();
		auto message_piece_header =
			MessagePiece::construct_message_piece_header(static_cast<uint32_t>(StarMessage::OPERATION_REPLICATION_REQUEST), message_size, 0, 0);

		Encoder encoder(message.data);
		encoder << message_piece_header;
		encoder << operation.tid;
		encoder.write_n_bytes(operation.data.c_str(), operation.data.size());
		message.flush();
		return message_size;
	}
};

template <class Database> class StarMessageHandler {
	using Transaction = SiloTransaction;

    public:
	static void async_value_replication_request_handler(MessagePiece inputPiece, Message &responseMessage, Database &db, Transaction *txn)
	{
		DCHECK(inputPiece.get_message_type() == static_cast<uint32_t>(StarMessage::ASYNC_VALUE_REPLICATION_REQUEST));
		auto table_id = inputPiece.get_table_id();
		auto partition_id = inputPiece.get_partition_id();
		ITable &table = *db.find_table(table_id, partition_id);
		DCHECK(table_id == table.tableID());
		DCHECK(partition_id == table.partitionID());
		auto key_size = table.key_size();
		auto field_size = table.field_size();

		/*
		 * The structure of an async value replication request:
		 *      (primary key, field value, commit_tid).
		 * The structure of an async value replication response: null
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

	static void sync_value_replication_request_handler(MessagePiece inputPiece, Message &responseMessage, Database &db, Transaction *txn)
	{
		DCHECK(inputPiece.get_message_type() == static_cast<uint32_t>(StarMessage::SYNC_VALUE_REPLICATION_REQUEST));
		auto table_id = inputPiece.get_table_id();
		auto partition_id = inputPiece.get_partition_id();
		ITable &table = *db.find_table(table_id, partition_id);
		DCHECK(table_id == table.tableID());
		DCHECK(partition_id == table.partitionID());
		auto key_size = table.key_size();
		auto field_size = table.field_size();

		/*
		 * The structure of a sync value replication request:
		 *      (primary key, field value, commit_tid).
		 * The structure of a sync value replication response: ()
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
		DCHECK(last_tid < commit_tid);
		table.deserialize_value(key, valueStringPiece);
		SiloHelper::unlock(tid, commit_tid);

		// prepare response message header
		auto message_size = MessagePiece::get_header_size();
		auto message_piece_header = MessagePiece::construct_message_piece_header(static_cast<uint32_t>(StarMessage::SYNC_VALUE_REPLICATION_RESPONSE),
											 message_size, table_id, partition_id);

		star::Encoder encoder(responseMessage.data);
		encoder << message_piece_header;
		responseMessage.flush();
	}

	static void sync_value_replication_response_handler(MessagePiece inputPiece, Message &responseMessage, Database &db, Transaction *txn)
	{
		DCHECK(inputPiece.get_message_type() == static_cast<uint32_t>(StarMessage::SYNC_VALUE_REPLICATION_RESPONSE));
		auto table_id = inputPiece.get_table_id();
		auto partition_id = inputPiece.get_partition_id();
		ITable &table = *db.find_table(table_id, partition_id);
		DCHECK(table_id == table.tableID());
		DCHECK(partition_id == table.partitionID());
		auto key_size = table.key_size();
		auto field_size = table.field_size();

		/*
		 * The structure of a sync value replication response: ()
		 */

		txn->pendingResponses--;
		txn->network_size += inputPiece.get_message_length();
	}

	static void operation_replication_request_handler(MessagePiece inputPiece, Message &responseMessage, Database &db, Transaction *txn)
	{
		DCHECK(inputPiece.get_message_type() == static_cast<uint32_t>(StarMessage::OPERATION_REPLICATION_REQUEST));

		auto message_size = inputPiece.get_message_length();
		Decoder dec(inputPiece.toStringPiece());
		Operation operation;
		dec >> operation.tid;

		auto data_size = message_size - MessagePiece::get_header_size() - sizeof(uint64_t);
		DCHECK(data_size > 0);

		operation.data.resize(data_size);
		dec.read_n_bytes(&operation.data[0], data_size);

		DCHECK(dec.size() == 0);
		db.apply_operation(operation);
	}

	static std::vector<std::function<void(MessagePiece, Message &, Database &, Transaction *)> > get_message_handlers()
	{
		std::vector<std::function<void(MessagePiece, Message &, Database &, Transaction *)> > v;
		v.resize(static_cast<int>(ControlMessage::NFIELDS));
		v.push_back(StarMessageHandler::async_value_replication_request_handler);
		v.push_back(StarMessageHandler::sync_value_replication_request_handler);
		v.push_back(StarMessageHandler::sync_value_replication_response_handler);
		v.push_back(StarMessageHandler::operation_replication_request_handler);
		return v;
	}
};
} // namespace star
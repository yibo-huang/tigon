//
// Created by Yi Lu on 9/11/18.
//

#pragma once
#include <unordered_set>
#include "common/Encoder.h"
#include "common/Message.h"
#include "common/MessagePiece.h"
#include "core/ControlMessage.h"
#include "core/Table.h"

#include "protocol/TwoPL/TwoPLHelper.h"
#include "protocol/TwoPL/TwoPLRWKey.h"
#include "protocol/TwoPL/TwoPLTransaction.h"

namespace star
{

enum class TwoPLMessage {
	READ_LOCK_REQUEST = static_cast<int>(ControlMessage::NFIELDS),
	READ_LOCK_RESPONSE,
	WRITE_LOCK_REQUEST,
	WRITE_LOCK_RESPONSE,
	ABORT_REQUEST,
	WRITE_REQUEST,
	WRITE_RESPONSE,
	REPLICATION_REQUEST,
	REPLICATION_RESPONSE,
	RELEASE_READ_LOCK_REQUEST,
	RELEASE_READ_LOCK_RESPONSE,
	RELEASE_WRITE_LOCK_REQUEST,
	RELEASE_WRITE_LOCK_RESPONSE,
	PREPARE_REQUEST,
	PREPARE_RESPONSE,
	PREPARE_REDO_REQUEST,
	PREPARE_REDO_RESPONSE,
	NFIELDS
};

class TwoPLMessageFactory {
    public:
	static std::size_t new_read_lock_message(Message &message, ITable &table, const void *key, uint32_t key_offset)
	{
		/*
		 * The structure of a read lock request: (primary key, key offset)
		 */

		auto key_size = table.key_size();

		auto message_size = MessagePiece::get_header_size() + key_size + sizeof(key_offset);
		auto message_piece_header = MessagePiece::construct_message_piece_header(static_cast<uint32_t>(TwoPLMessage::READ_LOCK_REQUEST), message_size,
											 table.tableID(), table.partitionID());

		Encoder encoder(message.data);
		encoder << message_piece_header;
		encoder.write_n_bytes(key, key_size);
		encoder << key_offset;
		message.flush();
		message.set_gen_time(Time::now());
		return message_size;
	}

	static std::size_t new_write_lock_message(Message &message, ITable &table, const void *key, uint32_t key_offset)
	{
		/*
		 * The structure of a write lock request: (primary key, key offset)
		 */

		auto key_size = table.key_size();

		auto message_size = MessagePiece::get_header_size() + key_size + sizeof(key_offset);
		auto message_piece_header = MessagePiece::construct_message_piece_header(static_cast<uint32_t>(TwoPLMessage::WRITE_LOCK_REQUEST), message_size,
											 table.tableID(), table.partitionID());

		Encoder encoder(message.data);
		encoder << message_piece_header;
		encoder.write_n_bytes(key, key_size);
		encoder << key_offset;
		message.flush();
		message.set_gen_time(Time::now());
		return message_size;
	}

	static std::size_t new_abort_message(Message &message, ITable &table, const void *key, bool write_lock)
	{
		/*
		 * The structure of an abort request: (primary key, wrtie lock)
		 */

		auto key_size = table.key_size();

		auto message_size = MessagePiece::get_header_size() + key_size + sizeof(bool);
		auto message_piece_header = MessagePiece::construct_message_piece_header(static_cast<uint32_t>(TwoPLMessage::ABORT_REQUEST), message_size,
											 table.tableID(), table.partitionID());

		Encoder encoder(message.data);
		encoder << message_piece_header;
		encoder.write_n_bytes(key, key_size);
		encoder << write_lock;
		message.flush();
		message.set_gen_time(Time::now());
		return message_size;
	}

	template <class DatabaseType>
	static std::size_t new_prepare_and_redo_message(Message &message, const std::vector<TwoPLRWKey> &redoWriteSet, DatabaseType &db)
	{
		auto message_size = MessagePiece::get_header_size();
		auto message_piece_header =
			MessagePiece::construct_message_piece_header(static_cast<uint32_t>(TwoPLMessage::PREPARE_REDO_REQUEST), message_size, 0, 0);

		Encoder encoder(message.data);
		size_t start_off = encoder.size();
		encoder << message_piece_header;
		encoder << redoWriteSet.size();
		for (size_t i = 0; i < redoWriteSet.size(); ++i) {
			auto writeKey = redoWriteSet[i];
			auto tableId = writeKey.get_table_id();
			auto partitionId = writeKey.get_partition_id();
			auto table = db.find_table(tableId, partitionId);
			auto key_size = table->key_size();
			auto value_size = table->value_size();
			auto key = writeKey.get_key();
			auto value = writeKey.get_value();
			encoder << tableId << partitionId << key_size;
			encoder.write_n_bytes(key, key_size);
			encoder << value_size;
			encoder.write_n_bytes(value, value_size);
		}

		message_size = encoder.size() - start_off;
		message_piece_header =
			MessagePiece::construct_message_piece_header(static_cast<uint32_t>(TwoPLMessage::PREPARE_REDO_REQUEST), message_size, 0, 0);
		encoder.replace_bytes_range(start_off, (void *)&message_piece_header, sizeof(message_piece_header));
		message.flush();
		return message_size;
	}

	static std::size_t new_prepare_message(Message &message, ITable &table)
	{
		/*
		 * The structure of a write request: (primary key, field value)
		 */
		auto message_size = MessagePiece::get_header_size();
		auto message_piece_header = MessagePiece::construct_message_piece_header(static_cast<uint32_t>(TwoPLMessage::PREPARE_REQUEST), message_size,
											 table.tableID(), table.partitionID());

		Encoder encoder(message.data);
		encoder << message_piece_header;
		message.flush();
		message.set_gen_time(Time::now());
		return message_size;
	}

	static std::size_t new_write_message(Message &message, ITable &table, const void *key, const void *value, uint64_t commit_tid,
					     bool persist_commit_record = false)
	{
		/*
		 * The structure of a write request: (primary key, field value)
		 */

		auto key_size = table.key_size();
		auto field_size = table.field_size();

		auto message_size = MessagePiece::get_header_size() + key_size + field_size + sizeof(commit_tid) + sizeof(persist_commit_record);
		auto message_piece_header = MessagePiece::construct_message_piece_header(static_cast<uint32_t>(TwoPLMessage::WRITE_REQUEST), message_size,
											 table.tableID(), table.partitionID());

		Encoder encoder(message.data);
		encoder << message_piece_header << commit_tid << persist_commit_record;
		encoder.write_n_bytes(key, key_size);
		table.serialize_value(encoder, value);
		message.flush();
		message.set_gen_time(Time::now());
		return message_size;
	}

	static std::size_t new_replication_message(Message &message, ITable &table, const void *key, const void *value, uint64_t commit_tid, bool sync_redo)
	{
		/*
		 * The structure of a replication request: (primary key, field value,
		 * commit_tid, sync_redo)
		 */

		auto key_size = table.key_size();
		auto field_size = table.field_size();

		auto message_size = MessagePiece::get_header_size() + key_size + field_size + sizeof(commit_tid) + sizeof(uint64_t) + sizeof(bool);
		auto message_piece_header = MessagePiece::construct_message_piece_header(static_cast<uint32_t>(TwoPLMessage::REPLICATION_REQUEST), message_size,
											 table.tableID(), table.partitionID());

		Encoder encoder(message.data);
		encoder << message_piece_header;
		encoder.write_n_bytes(key, key_size);
		table.serialize_value(encoder, value);
		encoder << commit_tid << sync_redo;
		uint64_t current_ts_micro =
			std::chrono::time_point_cast<std::chrono::microseconds>(std::chrono::steady_clock::now()).time_since_epoch().count();
		encoder << current_ts_micro;
		message.flush();
		message.set_gen_time(Time::now());
		return message_size;
	}

	static std::size_t new_release_read_lock_message(Message &message, ITable &table, const void *key)
	{
		/*
		 * The structure of a release read lock request: (primary key)
		 */

		auto key_size = table.key_size();

		auto message_size = MessagePiece::get_header_size() + key_size;
		auto message_piece_header = MessagePiece::construct_message_piece_header(static_cast<uint32_t>(TwoPLMessage::RELEASE_READ_LOCK_REQUEST),
											 message_size, table.tableID(), table.partitionID());

		Encoder encoder(message.data);
		encoder << message_piece_header;
		encoder.write_n_bytes(key, key_size);
		message.flush();
		message.set_gen_time(Time::now());
		return message_size;
	}

	static std::size_t new_release_write_lock_message(Message &message, ITable &table, const void *key, uint64_t commit_tid)
	{
		/*
		 * The structure of a release write lock request: (primary key, commit tid)
		 */

		auto key_size = table.key_size();

		auto message_size = MessagePiece::get_header_size() + key_size + sizeof(commit_tid);
		auto message_piece_header = MessagePiece::construct_message_piece_header(static_cast<uint32_t>(TwoPLMessage::RELEASE_WRITE_LOCK_REQUEST),
											 message_size, table.tableID(), table.partitionID());

		Encoder encoder(message.data);
		encoder << message_piece_header;
		encoder.write_n_bytes(key, key_size);
		encoder << commit_tid;
		message.flush();
		message.set_gen_time(Time::now());
		return message_size;
	}
};

class TwoPLMessageHandler {
	using Transaction = TwoPLTransaction;

    public:
	static void read_lock_request_handler(MessagePiece inputPiece, Message &responseMessage, ITable &table, Transaction *txn)
	{
		DCHECK(inputPiece.get_message_type() == static_cast<uint32_t>(TwoPLMessage::READ_LOCK_REQUEST));
		auto table_id = inputPiece.get_table_id();
		auto partition_id = inputPiece.get_partition_id();
		DCHECK(table_id == table.tableID());
		DCHECK(partition_id == table.partitionID());
		auto key_size = table.key_size();
		auto value_size = table.value_size();

		/*
		 * The structure of a read lock request: (primary key, key offset)
		 * The structure of a read lock response: (success?, key offset, value?,
		 * tid?)
		 */

		auto stringPiece = inputPiece.toStringPiece();
		uint32_t key_offset;

		DCHECK(inputPiece.get_message_length() == MessagePiece::get_header_size() + key_size + sizeof(key_offset));

		const void *key = stringPiece.data();
		auto row = table.search(key);
		std::atomic<uint64_t> &tid = *std::get<0>(row);

		stringPiece.remove_prefix(key_size);
		star::Decoder dec(stringPiece);
		dec >> key_offset;

		DCHECK(dec.size() == 0);

		bool success;
		uint64_t latest_tid = TwoPLHelper::read_lock(tid, success);

		// prepare response message header
		auto message_size = MessagePiece::get_header_size() + sizeof(bool) + sizeof(key_offset);

		if (success) {
			message_size += value_size + sizeof(uint64_t);
		}

		auto message_piece_header = MessagePiece::construct_message_piece_header(static_cast<uint32_t>(TwoPLMessage::READ_LOCK_RESPONSE), message_size,
											 table_id, partition_id);

		star::Encoder encoder(responseMessage.data);
		encoder << message_piece_header;
		encoder << success << key_offset;

		if (success) {
			// reserve size for read
			responseMessage.data.append(value_size, 0);
			void *dest = &responseMessage.data[0] + responseMessage.data.size() - value_size;
			// read to message buffer
			TwoPLHelper::read(row, dest, value_size);
			encoder << latest_tid;
		}

		responseMessage.flush();
		responseMessage.set_gen_time(Time::now());
	}

	static void read_lock_response_handler(MessagePiece inputPiece, Message &responseMessage, ITable &table, Transaction *txn)
	{
		DCHECK(inputPiece.get_message_type() == static_cast<uint32_t>(TwoPLMessage::READ_LOCK_RESPONSE));
		auto table_id = inputPiece.get_table_id();
		auto partition_id = inputPiece.get_partition_id();
		DCHECK(table_id == table.tableID());
		DCHECK(partition_id == table.partitionID());
		auto key_size = table.key_size();
		auto value_size = table.value_size();

		/*
		 * The structure of a read lock response: (success?, key offset, value?,
		 * tid?)
		 */

		bool success;
		uint32_t key_offset;

		StringPiece stringPiece = inputPiece.toStringPiece();
		Decoder dec(stringPiece);
		dec >> success >> key_offset;

		if (success) {
			DCHECK(inputPiece.get_message_length() ==
			       MessagePiece::get_header_size() + sizeof(success) + sizeof(key_offset) + value_size + sizeof(uint64_t));

			TwoPLRWKey &readKey = txn->readSet[key_offset];
			dec.read_n_bytes(readKey.get_value(), value_size);
			uint64_t tid;
			dec >> tid;
			readKey.set_read_lock_bit();
			readKey.set_tid(tid);
		} else {
			DCHECK(inputPiece.get_message_length() == MessagePiece::get_header_size() + sizeof(success) + sizeof(key_offset));

			txn->abort_lock = true;
		}

		txn->pendingResponses--;
		txn->network_size += inputPiece.get_message_length();
	}

	static void write_lock_request_handler(MessagePiece inputPiece, Message &responseMessage, ITable &table, Transaction *txn)
	{
		DCHECK(inputPiece.get_message_type() == static_cast<uint32_t>(TwoPLMessage::WRITE_LOCK_REQUEST));
		auto table_id = inputPiece.get_table_id();
		auto partition_id = inputPiece.get_partition_id();
		DCHECK(table_id == table.tableID());
		DCHECK(partition_id == table.partitionID());
		auto key_size = table.key_size();
		auto value_size = table.value_size();

		/*
		 * The structure of a write lock request: (primary key, key offset)
		 * The structure of a write lock response: (success?, key offset, value?,
		 * tid?)
		 */

		auto stringPiece = inputPiece.toStringPiece();
		uint32_t key_offset;

		DCHECK(inputPiece.get_message_length() == MessagePiece::get_header_size() + key_size + sizeof(key_offset));

		const void *key = stringPiece.data();
		auto row = table.search(key);
		std::atomic<uint64_t> &tid = *std::get<0>(row);

		stringPiece.remove_prefix(key_size);
		star::Decoder dec(stringPiece);
		dec >> key_offset;

		DCHECK(dec.size() == 0);

		bool success;
		uint64_t latest_tid = TwoPLHelper::write_lock(tid, success);

		// prepare response message header
		auto message_size = MessagePiece::get_header_size() + sizeof(bool) + sizeof(key_offset);

		if (success) {
			message_size += value_size + sizeof(uint64_t);
		}

		auto message_piece_header = MessagePiece::construct_message_piece_header(static_cast<uint32_t>(TwoPLMessage::WRITE_LOCK_RESPONSE), message_size,
											 table_id, partition_id);

		star::Encoder encoder(responseMessage.data);
		encoder << message_piece_header;
		encoder << success << key_offset;

		if (success) {
			// reserve size for read
			responseMessage.data.append(value_size, 0);
			void *dest = &responseMessage.data[0] + responseMessage.data.size() - value_size;
			// read to message buffer
			TwoPLHelper::read(row, dest, value_size);
			encoder << latest_tid;
		}
		responseMessage.set_gen_time(Time::now());
		responseMessage.flush();
	}

	static void write_lock_response_handler(MessagePiece inputPiece, Message &responseMessage, ITable &table, Transaction *txn)
	{
		DCHECK(inputPiece.get_message_type() == static_cast<uint32_t>(TwoPLMessage::WRITE_LOCK_RESPONSE));
		auto table_id = inputPiece.get_table_id();
		auto partition_id = inputPiece.get_partition_id();
		DCHECK(table_id == table.tableID());
		DCHECK(partition_id == table.partitionID());
		auto key_size = table.key_size();
		auto value_size = table.value_size();

		/*
		 * The structure of a read lock response: (success?, key offset, value?,
		 * tid?)
		 */

		bool success;
		uint32_t key_offset;

		StringPiece stringPiece = inputPiece.toStringPiece();
		Decoder dec(stringPiece);
		dec >> success >> key_offset;

		if (success) {
			DCHECK(inputPiece.get_message_length() ==
			       MessagePiece::get_header_size() + sizeof(success) + sizeof(key_offset) + value_size + sizeof(uint64_t));

			TwoPLRWKey &readKey = txn->readSet[key_offset];
			dec.read_n_bytes(readKey.get_value(), value_size);
			uint64_t tid;
			dec >> tid;
			readKey.set_write_lock_bit();
			readKey.set_tid(tid);
		} else {
			DCHECK(inputPiece.get_message_length() == MessagePiece::get_header_size() + sizeof(success) + sizeof(key_offset));

			txn->abort_lock = true;
		}

		txn->pendingResponses--;
		txn->network_size += inputPiece.get_message_length();
	}

	static void abort_request_handler(MessagePiece inputPiece, Message &responseMessage, ITable &table, Transaction *txn)
	{
		DCHECK(inputPiece.get_message_type() == static_cast<uint32_t>(TwoPLMessage::ABORT_REQUEST));
		auto table_id = inputPiece.get_table_id();
		auto partition_id = inputPiece.get_partition_id();
		DCHECK(table_id == table.tableID());
		DCHECK(partition_id == table.partitionID());
		auto key_size = table.key_size();

		/*
		 * The structure of an abort request: (primary key, write_lock)
		 * The structure of an abort response: null
		 */

		DCHECK(inputPiece.get_message_length() == MessagePiece::get_header_size() + key_size + sizeof(bool));

		auto stringPiece = inputPiece.toStringPiece();

		const void *key = stringPiece.data();
		stringPiece.remove_prefix(key_size);

		bool write_lock;
		Decoder dec(stringPiece);
		dec >> write_lock;

		std::atomic<uint64_t> *tid = table.search_metadata(key);
                CHECK(tid != nullptr);

		if (write_lock) {
			TwoPLHelper::write_lock_release(*tid);
		} else {
			TwoPLHelper::read_lock_release(*tid);
		}
	}

	static void write_request_handler(MessagePiece inputPiece, Message &responseMessage, ITable &table, Transaction *txn)
	{
		DCHECK(inputPiece.get_message_type() == static_cast<uint32_t>(TwoPLMessage::WRITE_REQUEST));
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

		DCHECK(inputPiece.get_message_length() == MessagePiece::get_header_size() + sizeof(uint64_t) + sizeof(bool) + key_size + field_size);

		Decoder dec(inputPiece.toStringPiece());
		uint64_t commit_tid;
		bool persist_commit_record;
		dec >> commit_tid >> persist_commit_record;
		auto stringPiece = dec.bytes;

		const void *key = stringPiece.data();
		stringPiece.remove_prefix(key_size);
		const void *value = stringPiece.data();
		table.deserialize_value(key, stringPiece);

		// prepare response message header
		auto message_size = MessagePiece::get_header_size();
		auto message_piece_header =
			MessagePiece::construct_message_piece_header(static_cast<uint32_t>(TwoPLMessage::WRITE_RESPONSE), message_size, table_id, partition_id);

		star::Encoder encoder(responseMessage.data);
		encoder << message_piece_header;
		responseMessage.flush();
		responseMessage.set_gen_time(Time::now());

		if (persist_commit_record) {
			DCHECK(txn->get_logger());
			std::ostringstream ss;
			ss << commit_tid << true;
			auto output = ss.str();
			auto lsn = txn->get_logger()->write(output.c_str(), output.size(), false);
			// txn->get_logger()->sync(lsn, );
		}
	}

	static void write_response_handler(MessagePiece inputPiece, Message &responseMessage, ITable &table, Transaction *txn)
	{
		DCHECK(inputPiece.get_message_type() == static_cast<uint32_t>(TwoPLMessage::WRITE_RESPONSE));
		auto table_id = inputPiece.get_table_id();
		auto partition_id = inputPiece.get_partition_id();
		DCHECK(table_id == table.tableID());
		DCHECK(partition_id == table.partitionID());
		auto key_size = table.key_size();

		/*
		 * The structure of a write response: ()
		 */

		txn->pendingResponses--;
		txn->network_size += inputPiece.get_message_length();
	}

	static void prepare_request_handler(MessagePiece inputPiece, Message &responseMessage, ITable &table, Transaction *txn)
	{
		DCHECK(inputPiece.get_message_type() == static_cast<uint32_t>(TwoPLMessage::PREPARE_REQUEST));
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

		DCHECK(inputPiece.get_message_length() == MessagePiece::get_header_size());

		// prepare response message header
		auto message_size = MessagePiece::get_header_size();
		auto message_piece_header = MessagePiece::construct_message_piece_header(static_cast<uint32_t>(TwoPLMessage::PREPARE_RESPONSE), message_size,
											 table_id, partition_id);

		star::Encoder encoder(responseMessage.data);
		encoder << message_piece_header;
		responseMessage.flush();
		responseMessage.set_gen_time(Time::now());
	}

	static void prepare_and_redo_request_handler(MessagePiece inputPiece, Message &responseMessage, ITable &table, Transaction *txn)
	{
		DCHECK(inputPiece.get_message_type() == static_cast<uint32_t>(TwoPLMessage::PREPARE_REDO_REQUEST));

		auto stringPiece = inputPiece.toStringPiece();
		Decoder dec(stringPiece);
		std::size_t redoWriteSetSize;

		bool success = true;

		dec >> redoWriteSetSize;

		DCHECK(txn->get_logger());

		std::string output;
		for (size_t i = 0; i < redoWriteSetSize; ++i) {
			uint64_t tableId;
			uint64_t partitionId;
			dec >> tableId >> partitionId;
			auto table = txn->getTable(tableId, partitionId);
			std::size_t key_size, value_size;
			uint64_t tid;
			dec >> key_size;
			DCHECK(key_size == table->key_size());
			const void *key = dec.get_raw_ptr();
			dec.remove_prefix(key_size);
			dec >> value_size;
			DCHECK(value_size == table->value_size());
			const void *value = dec.get_raw_ptr();
			dec.remove_prefix(value_size);

			std::ostringstream ss;
			ss << tableId << partitionId << key_size << std::string((char *)key, key_size) << value_size << std::string((char *)value, value_size);
			output += ss.str();
			// txn->get_logger()->write(output.c_str(), output.size(), false);
		}

		// prepare response message header
		auto message_size = MessagePiece::get_header_size() + sizeof(bool);
		auto message_piece_header =
			MessagePiece::construct_message_piece_header(static_cast<uint32_t>(TwoPLMessage::PREPARE_REDO_RESPONSE), message_size, 0, 0);

		star::Encoder encoder(responseMessage.data);
		encoder << message_piece_header;
		encoder << success;

		responseMessage.flush();
		// std::size_t lsn = 0;
		if (txn->get_logger()) {
			// write the vote
			std::ostringstream ss;
			ss << success;
			output += ss.str();
			txn->get_logger()->write(output.c_str(), output.size(), true);
		}

		if (txn->get_logger()) {
			// sync the vote and redo
			// On recovery, the txn is considered prepared only if all votes are true // passed all validation
			// txn->get_logger()->sync(lsn, );
		}
	}

	static void prepare_and_redo_response_handler(MessagePiece inputPiece, Message &responseMessage, ITable &table, Transaction *txn)
	{
		DCHECK(inputPiece.get_message_type() == static_cast<uint32_t>(TwoPLMessage::PREPARE_REDO_RESPONSE));

		bool success;

		Decoder dec(inputPiece.toStringPiece());

		dec >> success;

		txn->pendingResponses--;
		txn->network_size += inputPiece.get_message_length();

		DCHECK(success);
	}

	static void prepare_response_handler(MessagePiece inputPiece, Message &responseMessage, ITable &table, Transaction *txn)
	{
		DCHECK(inputPiece.get_message_type() == static_cast<uint32_t>(TwoPLMessage::PREPARE_RESPONSE));
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

		DCHECK(inputPiece.get_message_length() == MessagePiece::get_header_size());
		DCHECK(txn->pendingResponses > 0);
		txn->pendingResponses--;
	}

	static void replication_request_handler(MessagePiece inputPiece, Message &responseMessage, ITable &table, Transaction *txn)
	{
		DCHECK(inputPiece.get_message_type() == static_cast<uint32_t>(TwoPLMessage::REPLICATION_REQUEST));
		auto table_id = inputPiece.get_table_id();
		auto partition_id = inputPiece.get_partition_id();
		DCHECK(table_id == table.tableID());
		DCHECK(partition_id == table.partitionID());
		auto key_size = table.key_size();
		auto field_size = table.field_size();

		uint64_t current_ts_micro1 =
			std::chrono::time_point_cast<std::chrono::microseconds>(std::chrono::steady_clock::now()).time_since_epoch().count();
		/*
		 * The structure of a replication request: (primary key, field value, commit
		 * tid, sync_redo) The structure of a replication response: ()
		 */

		auto stringPiece = inputPiece.toStringPiece();

		const void *key = stringPiece.data();
		stringPiece.remove_prefix(key_size);
		auto valueStringPiece = stringPiece;
		stringPiece.remove_prefix(field_size);

		uint64_t commit_tid, req_timestamp;
		bool sync_redo = false;
		Decoder dec(stringPiece);
		dec >> commit_tid >> sync_redo >> req_timestamp;

		DCHECK(dec.size() == 0);

		std::atomic<uint64_t> *tid = table.search_metadata(key);
                CHECK(tid != nullptr);

		uint64_t last_tid = TwoPLHelper::write_lock(*tid);
		DCHECK(last_tid < commit_tid);
		table.deserialize_value(key, valueStringPiece);
		TwoPLHelper::write_lock_release(*tid, commit_tid);

		// uint64_t lsn = 0;
		//  if (txn->get_logger()) {
		//    std::ostringstream ss;
		//    ss << commit_tid << std::string((const char *)key, key_size) << std::string(valueStringPiece.data(), field_size);
		//    auto output = ss.str();
		//    txn->get_logger()->write(output.c_str(), output.size(), sync_redo);
		//  }

		// if (txn->get_logger() && sync_redo) {
		//   txn->get_logger()->sync(lsn, );
		// }

		// prepare response message header
		auto message_size = MessagePiece::get_header_size() + sizeof(uint64_t) * 3;
		auto message_piece_header = MessagePiece::construct_message_piece_header(static_cast<uint32_t>(TwoPLMessage::REPLICATION_RESPONSE),
											 message_size, table_id, partition_id);

		uint64_t current_ts_micro2 =
			std::chrono::time_point_cast<std::chrono::microseconds>(std::chrono::steady_clock::now()).time_since_epoch().count();
		star::Encoder encoder(responseMessage.data);
		encoder << message_piece_header << req_timestamp << current_ts_micro1 << current_ts_micro2;
		responseMessage.flush();
		responseMessage.set_gen_time(Time::now());
	}

	static void replication_response_handler(MessagePiece inputPiece, Message &responseMessage, ITable &table, Transaction *txn)
	{
		DCHECK(inputPiece.get_message_type() == static_cast<uint32_t>(TwoPLMessage::REPLICATION_RESPONSE));
		auto table_id = inputPiece.get_table_id();
		auto partition_id = inputPiece.get_partition_id();
		DCHECK(table_id == table.tableID());
		DCHECK(partition_id == table.partitionID());
		auto key_size = table.key_size();

		auto stringPiece = inputPiece.toStringPiece();
		uint64_t req_ts, req_recv_ts, response_generate_ts;
		Decoder dec(stringPiece);
		dec >> req_ts >> req_recv_ts >> response_generate_ts;
		uint64_t current_ts_micro =
			std::chrono::time_point_cast<std::chrono::microseconds>(std::chrono::steady_clock::now()).time_since_epoch().count();

		// LOG(INFO) << "Txn " << (uint64_t)txn << " req_ts " << req_ts
		//           << " resp_recv_ts " << req_recv_ts
		//           << " resp_gen_ts " << response_generate_ts
		//           << " cur_ts " << current_ts_micro;
		/*
		 * The structure of a replication response: ()
		 */

		txn->pendingResponses--;
		txn->network_size += inputPiece.get_message_length();
	}

	static void release_read_lock_request_handler(MessagePiece inputPiece, Message &responseMessage, ITable &table, Transaction *txn)
	{
		DCHECK(inputPiece.get_message_type() == static_cast<uint32_t>(TwoPLMessage::RELEASE_READ_LOCK_REQUEST));
		auto table_id = inputPiece.get_table_id();
		auto partition_id = inputPiece.get_partition_id();
		DCHECK(table_id == table.tableID());
		DCHECK(partition_id == table.partitionID());
		auto key_size = table.key_size();

		/*
		 * The structure of a release read lock request: (primary key)
		 * The structure of a release read lock response: ()
		 */

		DCHECK(inputPiece.get_message_length() == MessagePiece::get_header_size() + key_size);

		auto stringPiece = inputPiece.toStringPiece();

		const void *key = stringPiece.data();
		stringPiece.remove_prefix(key_size);

		std::atomic<uint64_t> *tid = table.search_metadata(key);
                CHECK(tid != nullptr);

		TwoPLHelper::read_lock_release(*tid);

		// prepare response message header
		auto message_size = MessagePiece::get_header_size();
		auto message_piece_header = MessagePiece::construct_message_piece_header(static_cast<uint32_t>(TwoPLMessage::RELEASE_READ_LOCK_RESPONSE),
											 message_size, table_id, partition_id);

		star::Encoder encoder(responseMessage.data);
		encoder << message_piece_header;
		responseMessage.flush();
		responseMessage.set_gen_time(Time::now());
	}

	static void release_read_lock_response_handler(MessagePiece inputPiece, Message &responseMessage, ITable &table, Transaction *txn)
	{
		DCHECK(inputPiece.get_message_type() == static_cast<uint32_t>(TwoPLMessage::RELEASE_READ_LOCK_RESPONSE));
		auto table_id = inputPiece.get_table_id();
		auto partition_id = inputPiece.get_partition_id();
		DCHECK(table_id == table.tableID());
		DCHECK(partition_id == table.partitionID());
		auto key_size = table.key_size();

		/*
		 * The structure of a release read lock response: ()
		 */

		// txn->pendingResponses--;
		txn->network_size += inputPiece.get_message_length();
	}

	static void release_write_lock_request_handler(MessagePiece inputPiece, Message &responseMessage, ITable &table, Transaction *txn)
	{
		DCHECK(inputPiece.get_message_type() == static_cast<uint32_t>(TwoPLMessage::RELEASE_WRITE_LOCK_REQUEST));
		auto table_id = inputPiece.get_table_id();
		auto partition_id = inputPiece.get_partition_id();
		DCHECK(table_id == table.tableID());
		DCHECK(partition_id == table.partitionID());
		auto key_size = table.key_size();
		/*
		 * The structure of a release write lock request: (primary key, commit tid)
		 * The structure of a release write lock response: ()
		 */

		DCHECK(inputPiece.get_message_length() == MessagePiece::get_header_size() + key_size + sizeof(uint64_t));

		auto stringPiece = inputPiece.toStringPiece();

		const void *key = stringPiece.data();
		stringPiece.remove_prefix(key_size);

		uint64_t commit_tid;
		Decoder dec(stringPiece);
		dec >> commit_tid;

		std::atomic<uint64_t> *tid = table.search_metadata(key);
                CHECK(tid != nullptr);
		TwoPLHelper::write_lock_release(*tid, commit_tid);

		// prepare response message header
		auto message_size = MessagePiece::get_header_size();
		auto message_piece_header = MessagePiece::construct_message_piece_header(static_cast<uint32_t>(TwoPLMessage::RELEASE_WRITE_LOCK_RESPONSE),
											 message_size, table_id, partition_id);

		star::Encoder encoder(responseMessage.data);
		encoder << message_piece_header;
		responseMessage.flush();
		responseMessage.set_gen_time(Time::now());
	}

	static void release_write_lock_response_handler(MessagePiece inputPiece, Message &responseMessage, ITable &table, Transaction *txn)
	{
		DCHECK(inputPiece.get_message_type() == static_cast<uint32_t>(TwoPLMessage::RELEASE_WRITE_LOCK_RESPONSE));
		auto table_id = inputPiece.get_table_id();
		auto partition_id = inputPiece.get_partition_id();
		DCHECK(table_id == table.tableID());
		DCHECK(partition_id == table.partitionID());
		auto key_size = table.key_size();

		/*
		 * The structure of a release write lock response: ()
		 */

		// txn->pendingResponses--;
		txn->network_size += inputPiece.get_message_length();
	}

    public:
	static std::vector<std::function<void(MessagePiece, Message &, ITable &, Transaction *)> > get_message_handlers()
	{
		std::vector<std::function<void(MessagePiece, Message &, ITable &, Transaction *)> > v;
		v.resize(static_cast<int>(ControlMessage::NFIELDS));
		v.push_back(read_lock_request_handler);
		v.push_back(read_lock_response_handler);
		v.push_back(write_lock_request_handler);
		v.push_back(write_lock_response_handler);
		v.push_back(abort_request_handler);
		v.push_back(write_request_handler);
		v.push_back(write_response_handler);
		v.push_back(replication_request_handler);
		v.push_back(replication_response_handler);
		v.push_back(release_read_lock_request_handler);
		v.push_back(release_read_lock_response_handler);
		v.push_back(release_write_lock_request_handler);
		v.push_back(release_write_lock_response_handler);
		v.push_back(prepare_request_handler);
		v.push_back(prepare_response_handler);
		v.push_back(prepare_and_redo_request_handler);
		v.push_back(prepare_and_redo_response_handler);
		return v;
	}
};

} // namespace star

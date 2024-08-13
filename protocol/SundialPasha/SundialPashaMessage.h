//
// Created by Xinjing Zhou Lu on 04/26/22.
//

#pragma once

#include "common/Encoder.h"
#include "common/Message.h"
#include "common/MessagePiece.h"
#include "core/ControlMessage.h"
#include "core/Table.h"

#include "protocol/SundialPasha/SundialPashaHelper.h"
#include "protocol/SundialPasha/SundialPashaRWKey.h"
#include "protocol/SundialPasha/SundialPashaTransaction.h"

namespace star
{

enum class SundialPashaMessage {
	SEARCH_REQUEST = static_cast<int>(ControlMessage::NFIELDS),
	SEARCH_RESPONSE,
	LOCK_REQUEST,
	LOCK_RESPONSE,
	READ_VALIDATION_REQUEST,
	READ_VALIDATION_RESPONSE,
	READ_VALIDATION_AND_REDO_REQUEST,
	READ_VALIDATION_AND_REDO_RESPONSE,
	ABORT_REQUEST,
	WRITE_REQUEST,
	WRITE_RESPONSE,
	REPLICATION_REQUEST,
	REPLICATION_RESPONSE,
	RELEASE_LOCK_REQUEST,
	READ_REQUEST,
	READ_RESPONSE,
	WRITE_LOCK_REQUEST,
	WRITE_LOCK_RESPONSE,
	UNLOCK_REQUEST,
	UPDATE_UNLOCK_REQUEST,
	RENEW_LEASE_REQUEST,
	RENEW_LEASE_RESPONSE,
	NFIELDS
};

class SundialPashaMessageFactory {
    public:
	static std::size_t new_read_message(Message &message, ITable &table, const void *key, uint64_t transaction_id, bool write_lock, uint32_t key_offset)
	{
		/*
		 * The structure of a read request: (primary key, write_lock, read key offset)
		 */

		auto key_size = table.key_size();

		auto message_size = MessagePiece::get_header_size() + key_size + sizeof(transaction_id) + sizeof(write_lock) + sizeof(key_offset);
		auto message_piece_header = MessagePiece::construct_message_piece_header(static_cast<uint32_t>(SundialPashaMessage::READ_REQUEST), message_size,
											 table.tableID(), table.partitionID());

		Encoder encoder(message.data);
		encoder << message_piece_header;
		encoder.write_n_bytes(key, key_size);
		encoder << transaction_id;
		encoder << write_lock;
		encoder << key_offset;
		message.flush();
		message.set_gen_time(Time::now());
		return message_size;
	}

	static std::size_t new_write_lock_message(Message &message, ITable &table, uint64_t transaction_id, const void *key, uint32_t key_offset)
	{
		/*
		 * The structure of a read request: (primary key, write key offset)
		 */

		auto key_size = table.key_size();

		auto message_size = MessagePiece::get_header_size() + sizeof(transaction_id) + key_size + sizeof(key_offset);
		auto message_piece_header = MessagePiece::construct_message_piece_header(static_cast<uint32_t>(SundialPashaMessage::WRITE_LOCK_REQUEST),
											 message_size, table.tableID(), table.partitionID());

		Encoder encoder(message.data);
		encoder << message_piece_header;
		encoder.write_n_bytes(key, key_size);
		encoder << transaction_id;
		encoder << key_offset;
		message.flush();
		message.set_gen_time(Time::now());
		return message_size;
	}

	static std::size_t new_renew_lease_message(Message &message, ITable &table, const void *key, uint32_t key_offset, uint64_t wts, uint64_t commit_ts)
	{
		/*
		 * The structure of a read request: (primary key, wts, commit_ts, read key offset)
		 */

		auto key_size = table.key_size();

		auto message_size = MessagePiece::get_header_size() + key_size + sizeof(wts) + sizeof(commit_ts) + sizeof(key_offset);
		auto message_piece_header = MessagePiece::construct_message_piece_header(static_cast<uint32_t>(SundialPashaMessage::RENEW_LEASE_REQUEST),
											 message_size, table.tableID(), table.partitionID());

		Encoder encoder(message.data);
		encoder << message_piece_header;
		encoder.write_n_bytes(key, key_size);
		encoder << wts;
		encoder << commit_ts;
		encoder << key_offset;
		message.flush();
		message.set_gen_time(Time::now());
		return message_size;
	}

	static std::size_t new_unlock_message(Message &message, ITable &table, const void *key, uint64_t transaction_id, uint32_t key_offset)
	{
		/*
		 * The structure of a unlock request: (primary key, transaction_id, write key offset)
		 */

		auto key_size = table.key_size();

		auto message_size = MessagePiece::get_header_size() + key_size + sizeof(transaction_id) + sizeof(key_offset);
		auto message_piece_header = MessagePiece::construct_message_piece_header(static_cast<uint32_t>(SundialPashaMessage::UNLOCK_REQUEST), message_size,
											 table.tableID(), table.partitionID());

		Encoder encoder(message.data);
		encoder << message_piece_header;
		encoder.write_n_bytes(key, key_size);
		encoder << transaction_id;
		encoder << key_offset;
		message.flush();
		message.set_gen_time(Time::now());
		return message_size;
	}

	static std::size_t new_update_and_unlock_message(Message &message, ITable &table, const void *key, const void *value, uint64_t transaction_id,
							 uint64_t commit_ts, uint32_t key_offset)
	{
		/*
		 * The structure of a update and unlock request: (primary key, value, transaction_id, write key offset)
		 */

		auto key_size = table.key_size();
		auto value_size = table.value_size();

		auto message_size = MessagePiece::get_header_size() + key_size + value_size + sizeof(transaction_id) + sizeof(key_offset) + sizeof(commit_ts);
		auto message_piece_header = MessagePiece::construct_message_piece_header(static_cast<uint32_t>(SundialPashaMessage::UPDATE_UNLOCK_REQUEST),
											 message_size, table.tableID(), table.partitionID());

		Encoder encoder(message.data);
		encoder << message_piece_header;
		encoder.write_n_bytes(key, key_size);
		table.serialize_value(encoder, value);
		encoder << transaction_id;
		encoder << commit_ts;
		encoder << key_offset;
		message.flush();
		message.set_gen_time(Time::now());
		return message_size;
	}

	static std::size_t new_search_message(Message &message, ITable &table, const void *key, uint32_t key_offset)
	{
		/*
		 * The structure of a search request: (primary key, read key offset)
		 */

		auto key_size = table.key_size();

		auto message_size = MessagePiece::get_header_size() + key_size + sizeof(key_offset);
		auto message_piece_header = MessagePiece::construct_message_piece_header(static_cast<uint32_t>(SundialPashaMessage::SEARCH_REQUEST), message_size,
											 table.tableID(), table.partitionID());

		Encoder encoder(message.data);
		encoder << message_piece_header;
		encoder.write_n_bytes(key, key_size);
		encoder << key_offset;
		message.flush();
		message.set_gen_time(Time::now());
		return message_size;
	}

	static std::size_t new_lock_message(Message &message, ITable &table, const void *key, uint32_t key_offset)
	{
		/*
		 * The structure of a lock request: (primary key, write key offset)
		 */

		auto key_size = table.key_size();

		auto message_size = MessagePiece::get_header_size() + key_size + sizeof(key_offset);
		auto message_piece_header = MessagePiece::construct_message_piece_header(static_cast<uint32_t>(SundialPashaMessage::LOCK_REQUEST), message_size,
											 table.tableID(), table.partitionID());

		Encoder encoder(message.data);
		encoder << message_piece_header;
		encoder.write_n_bytes(key, key_size);
		encoder << key_offset;
		message.flush();
		message.set_gen_time(Time::now());
		return message_size;
	}

	template <class DatabaseType>
	static std::size_t new_read_validation_and_redo_message(Message &message, const std::vector<SundialPashaRWKey> &validationReadSet,
								const std::vector<SundialPashaRWKey> &redoWriteSet, uint64_t commit_ts, DatabaseType &db)
	{
		/*
		 * The structure of a read validation request: (read_pk1_table_id, read_pk1_partition_id, read_pk1_size, read_pk1, read_pk2_table_id)
		 */
		auto message_size = MessagePiece::get_header_size();
		auto message_piece_header = MessagePiece::construct_message_piece_header(
			static_cast<uint32_t>(SundialPashaMessage::READ_VALIDATION_AND_REDO_REQUEST), message_size, 0, 0);

		Encoder encoder(message.data);
		size_t start_off = encoder.size();
		encoder << message_piece_header;
		encoder << validationReadSet.size();
		encoder << commit_ts;

		for (size_t i = 0; i < validationReadSet.size(); ++i) {
			auto readKey = validationReadSet[i];
			auto tableId = readKey.get_table_id();
			auto partitionId = readKey.get_partition_id();
			auto table = db.find_table(tableId, partitionId);
			auto key_size = table->key_size();
			auto value_size = table->value_size();
			auto key = readKey.get_key();
			auto tid = readKey.get_tid();
			encoder << tableId << partitionId << key_size;
			encoder.write_n_bytes(key, key_size);
			encoder << readKey.get_wts();
		}

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
		message_piece_header = MessagePiece::construct_message_piece_header(static_cast<uint32_t>(SundialPashaMessage::READ_VALIDATION_AND_REDO_REQUEST),
										    message_size, 0, 0);
		encoder.replace_bytes_range(start_off, (void *)&message_piece_header, sizeof(message_piece_header));
		message.flush();
		return message_size;
	}

	static std::size_t new_read_validation_message(Message &message, ITable &table, const void *key, uint32_t key_offset, uint64_t tid)
	{
		/*
		 * The structure of a read validation request: (primary key, read key
		 * offset, tid)
		 */

		auto key_size = table.key_size();

		auto message_size = MessagePiece::get_header_size() + key_size + sizeof(key_offset) + sizeof(tid);
		auto message_piece_header = MessagePiece::construct_message_piece_header(static_cast<uint32_t>(SundialPashaMessage::READ_VALIDATION_REQUEST),
											 message_size, table.tableID(), table.partitionID());

		Encoder encoder(message.data);
		encoder << message_piece_header;
		encoder.write_n_bytes(key, key_size);
		encoder << key_offset << tid;
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
		auto message_piece_header = MessagePiece::construct_message_piece_header(static_cast<uint32_t>(SundialPashaMessage::ABORT_REQUEST), message_size,
											 table.tableID(), table.partitionID());

		Encoder encoder(message.data);
		encoder << message_piece_header;
		encoder.write_n_bytes(key, key_size);
		message.flush();
		message.set_gen_time(Time::now());
		return message_size;
	}

	static std::size_t new_write_message(Message &message, ITable &table, const void *key, const void *value, uint64_t commit_ts, uint64_t transaction_id,
					     bool persist_commit_record = false)
	{
		/*
		 * The structure of a write request: (commit_tid, persist_commit_record?, primary key, field value)
		 */

		auto key_size = table.key_size();
		auto field_size = table.field_size();

		auto message_size = MessagePiece::get_header_size() + sizeof(commit_ts) + sizeof(transaction_id) + sizeof(bool) + key_size + field_size;
		auto message_piece_header = MessagePiece::construct_message_piece_header(static_cast<uint32_t>(SundialPashaMessage::WRITE_REQUEST), message_size,
											 table.tableID(), table.partitionID());

		Encoder encoder(message.data);
		encoder << message_piece_header << commit_ts << transaction_id << persist_commit_record;
		encoder.write_n_bytes(key, key_size);
		table.serialize_value(encoder, value);
		message.flush();
		message.set_gen_time(Time::now());
		return message_size;
	}

	static std::size_t new_replication_message(Message &message, ITable &table, const void *key, const void *value, uint64_t commit_ts, bool sync_redo)
	{
		/*
		 * The structure of a replication request: (primary key, field value,
		 * commit_ts)
		 */

		auto key_size = table.key_size();
		auto field_size = table.field_size();

		auto message_size = MessagePiece::get_header_size() + key_size + field_size + sizeof(commit_ts) + sizeof(bool);
		auto message_piece_header = MessagePiece::construct_message_piece_header(static_cast<uint32_t>(SundialPashaMessage::REPLICATION_REQUEST),
											 message_size, table.tableID(), table.partitionID());

		Encoder encoder(message.data);
		encoder << message_piece_header;
		encoder.write_n_bytes(key, key_size);
		table.serialize_value(encoder, value);
		encoder << commit_ts << sync_redo;
		message.flush();
		message.set_gen_time(Time::now());
		return message_size;
	}

	static std::size_t new_release_lock_message(Message &message, ITable &table, const void *key, uint64_t commit_tid)
	{
		/*
		 * The structure of a replication request: (primary key, commit tid)
		 */

		auto key_size = table.key_size();

		auto message_size = MessagePiece::get_header_size() + key_size + sizeof(commit_tid);
		auto message_piece_header = MessagePiece::construct_message_piece_header(static_cast<uint32_t>(SundialPashaMessage::RELEASE_LOCK_REQUEST),
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

class SundialPashaMessageHandler {
	using Transaction = SundialPashaTransaction;

    public:
	static void search_request_handler(MessagePiece inputPiece, Message &responseMessage, ITable &table, Transaction *txn)
	{
		DCHECK(inputPiece.get_message_type() == static_cast<uint32_t>(SundialPashaMessage::SEARCH_REQUEST));
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
		auto message_piece_header = MessagePiece::construct_message_piece_header(static_cast<uint32_t>(SundialPashaMessage::SEARCH_RESPONSE), message_size,
											 table_id, partition_id);

		star::Encoder encoder(responseMessage.data);
		encoder << message_piece_header;

		// reserve size for read
		responseMessage.data.append(value_size, 0);
		void *dest = &responseMessage.data[0] + responseMessage.data.size() - value_size;
		// read to message buffer
		auto tid = SundialPashaHelper::read(row, dest, value_size);

		encoder << tid << key_offset;
		responseMessage.flush();
	}

	static void search_response_handler(MessagePiece inputPiece, Message &responseMessage, ITable &table, Transaction *txn)
	{
		DCHECK(inputPiece.get_message_type() == static_cast<uint32_t>(SundialPashaMessage::SEARCH_RESPONSE));
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

		SundialPashaRWKey &readKey = txn->readSet[key_offset];
		dec = Decoder(inputPiece.toStringPiece());
		dec.read_n_bytes(readKey.get_value(), value_size);
		readKey.set_tid(tid);
		txn->pendingResponses--;
		txn->network_size += inputPiece.get_message_length();
	}

	static void lock_request_handler(MessagePiece inputPiece, Message &responseMessage, ITable &table, Transaction *txn)
	{
		DCHECK(inputPiece.get_message_type() == static_cast<uint32_t>(SundialPashaMessage::LOCK_REQUEST));
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
		std::atomic<uint64_t> &tid = table.search_metadata(key);

		bool success;
		uint64_t latest_tid = SundialPashaHelper::lock(tid, success);

		stringPiece.remove_prefix(key_size);
		star::Decoder dec(stringPiece);
		dec >> key_offset;

		DCHECK(dec.size() == 0);

		// prepare response message header
		auto message_size = MessagePiece::get_header_size() + sizeof(bool) + sizeof(uint64_t) + sizeof(uint32_t);
		auto message_piece_header = MessagePiece::construct_message_piece_header(static_cast<uint32_t>(SundialPashaMessage::LOCK_RESPONSE), message_size,
											 table_id, partition_id);

		star::Encoder encoder(responseMessage.data);
		encoder << message_piece_header;
		encoder << success << latest_tid << key_offset;
		responseMessage.flush();
		responseMessage.set_gen_time(Time::now());
	}

	static void lock_response_handler(MessagePiece inputPiece, Message &responseMessage, ITable &table, Transaction *txn)
	{
		DCHECK(inputPiece.get_message_type() == static_cast<uint32_t>(SundialPashaMessage::LOCK_RESPONSE));
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

		SundialPashaRWKey &writeKey = txn->writeSet[key_offset];

		bool tid_changed = false;

		if (success) {
			SundialPashaRWKey *readKey = txn->get_read_key(writeKey.get_key());

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

	static void read_validation_and_redo_request_handler(MessagePiece inputPiece, Message &responseMessage, ITable &table, Transaction *txn)
	{
		DCHECK(inputPiece.get_message_type() == static_cast<uint32_t>(SundialPashaMessage::READ_VALIDATION_AND_REDO_REQUEST));
		// std::size_t lsn = 0;
		/*
		 * The structure of a read validation request: (primary key, read key
		 * offset, tid, last_validation) The structure of a read validation response: (success?, read
		 * key offset)
		 */

		auto stringPiece = inputPiece.toStringPiece();
		Decoder dec(stringPiece);
		std::size_t validationReadSetSize;
		std::size_t redoWriteSetSize;
		uint64_t commit_ts;
		dec >> validationReadSetSize;
		dec >> commit_ts;

		bool success = true;

		for (size_t i = 0; i < validationReadSetSize; ++i) {
			uint64_t tableId;
			uint64_t partitionId;
			dec >> tableId >> partitionId;
			auto table = txn->getTable(tableId, partitionId);
			std::size_t key_size;
			uint64_t wts;
			dec >> key_size;
			DCHECK(key_size == table->key_size());
			const void *key = dec.get_raw_ptr();
			dec.remove_prefix(key_size);
			dec >> wts;

			auto row = table->search(key);
			bool res = SundialPashaHelper::renew_lease(row, wts, commit_ts);

			if (res == false) { // renew_lease failed
				success = false;
			}
		}

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
		}

		DCHECK(dec.size() == 0);

		// prepare response message header
		auto message_size = MessagePiece::get_header_size() + sizeof(bool);
		auto message_piece_header = MessagePiece::construct_message_piece_header(
			static_cast<uint32_t>(SundialPashaMessage::READ_VALIDATION_AND_REDO_RESPONSE), message_size, 0, 0);

		star::Encoder encoder(responseMessage.data);
		encoder << message_piece_header;
		encoder << success;

		responseMessage.flush();

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

	static void read_validation_request_handler(MessagePiece inputPiece, Message &responseMessage, ITable &table, Transaction *txn)
	{
		DCHECK(false);
		DCHECK(inputPiece.get_message_type() == static_cast<uint32_t>(SundialPashaMessage::READ_VALIDATION_REQUEST));
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

		DCHECK(inputPiece.get_message_length() == MessagePiece::get_header_size() + key_size + sizeof(uint32_t) + sizeof(uint64_t));

		auto stringPiece = inputPiece.toStringPiece();
		const void *key = stringPiece.data();
		auto latest_tid = table.search_metadata(key).load();
		stringPiece.remove_prefix(key_size);

		uint32_t key_offset;
		uint64_t tid;
		bool last_validation;
		Decoder dec(stringPiece);
		dec >> key_offset >> tid;

		bool success = true;

		if (SundialPashaHelper::remove_lock_bit(latest_tid) != tid) {
			success = false;
		}

		if (SundialPashaHelper::is_locked(latest_tid)) { // must be locked by others
			success = false;
		}

		// prepare response message header
		auto message_size = MessagePiece::get_header_size() + sizeof(bool) + sizeof(uint32_t);
		auto message_piece_header = MessagePiece::construct_message_piece_header(static_cast<uint32_t>(SundialPashaMessage::READ_VALIDATION_RESPONSE),
											 message_size, table_id, partition_id);

		star::Encoder encoder(responseMessage.data);
		encoder << message_piece_header;
		encoder << success << key_offset;

		responseMessage.flush();
	}

	static void read_validation_response_handler(MessagePiece inputPiece, Message &responseMessage, ITable &table, Transaction *txn)
	{
		DCHECK(false);
		DCHECK(inputPiece.get_message_type() == static_cast<uint32_t>(SundialPashaMessage::READ_VALIDATION_RESPONSE));
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

		SundialPashaRWKey &readKey = txn->readSet[key_offset];

		txn->pendingResponses--;
		txn->network_size += inputPiece.get_message_length();

		if (!success) {
			txn->abort_read_validation = true;
		}
	}

	static void read_validation_and_redo_response_handler(MessagePiece inputPiece, Message &responseMessage, ITable &table, Transaction *txn)
	{
		DCHECK(inputPiece.get_message_type() == static_cast<uint32_t>(SundialPashaMessage::READ_VALIDATION_AND_REDO_RESPONSE));

		/*
		 * The structure of a read validation response: (success?, read key offset)
		 */

		bool success;

		Decoder dec(inputPiece.toStringPiece());

		dec >> success;

		txn->pendingResponses--;
		txn->network_size += inputPiece.get_message_length();

		if (!success) {
			txn->abort_read_validation = true;
		}
	}

	static void abort_request_handler(MessagePiece inputPiece, Message &responseMessage, ITable &table, Transaction *txn)
	{
		DCHECK(inputPiece.get_message_type() == static_cast<uint32_t>(SundialPashaMessage::ABORT_REQUEST));
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
		std::atomic<uint64_t> &tid = table.search_metadata(key);

		// unlock the key
		SundialPashaHelper::unlock(tid);
	}

	static void write_request_handler(MessagePiece inputPiece, Message &responseMessage, ITable &table, Transaction *txn)
	{
		DCHECK(inputPiece.get_message_type() == static_cast<uint32_t>(SundialPashaMessage::WRITE_REQUEST));
		auto table_id = inputPiece.get_table_id();
		auto partition_id = inputPiece.get_partition_id();
		DCHECK(table_id == table.tableID());
		DCHECK(partition_id == table.partitionID());
		auto key_size = table.key_size();
		auto field_size = table.field_size();

		/*
		 * The structure of a write request: (commit_tid, persist_commit_record, primary key, field value)
		 * The structure of a write response: ()
		 */

		DCHECK(inputPiece.get_message_length() ==
		       MessagePiece::get_header_size() + sizeof(uint64_t) + sizeof(uint64_t) + sizeof(bool) + key_size + field_size);

		Decoder dec(inputPiece.toStringPiece());
		uint64_t commit_ts;
		uint64_t transaction_id;
		bool persist_commit_record;
		dec >> commit_ts >> transaction_id >> persist_commit_record;
		auto stringPiece = dec.bytes;

		const void *key = stringPiece.data();
		stringPiece.remove_prefix(key_size);
		const void *value = stringPiece.data();
		auto row = table.search(key);
		SundialPashaHelper::update(row, value, field_size, commit_ts, transaction_id);

		// prepare response message header
		auto message_size = MessagePiece::get_header_size();
		auto message_piece_header = MessagePiece::construct_message_piece_header(static_cast<uint32_t>(SundialPashaMessage::WRITE_RESPONSE), message_size,
											 table_id, partition_id);

		star::Encoder encoder(responseMessage.data);
		encoder << message_piece_header;
		responseMessage.flush();
		responseMessage.set_gen_time(Time::now());

		if (persist_commit_record) {
			DCHECK(txn->get_logger());
			std::ostringstream ss;
			ss << commit_ts << true;
			auto output = ss.str();
			auto lsn = txn->get_logger()->write(output.c_str(), output.size(), false);
			// txn->get_logger()->sync(lsn, );
		}
	}

	static void write_response_handler(MessagePiece inputPiece, Message &responseMessage, ITable &table, Transaction *txn)
	{
		DCHECK(inputPiece.get_message_type() == static_cast<uint32_t>(SundialPashaMessage::WRITE_RESPONSE));
		auto table_id = inputPiece.get_table_id();
		auto partition_id = inputPiece.get_partition_id();
		DCHECK(table_id == table.tableID());
		DCHECK(partition_id == table.partitionID());

		/*
		 * The structure of a write response: ()
		 */

		txn->pendingResponses--;
		txn->network_size += inputPiece.get_message_length();
	}

	static void replication_request_handler(MessagePiece inputPiece, Message &responseMessage, ITable &table, Transaction *txn)
	{
		DCHECK(inputPiece.get_message_type() == static_cast<uint32_t>(SundialPashaMessage::REPLICATION_REQUEST));
		auto table_id = inputPiece.get_table_id();
		auto partition_id = inputPiece.get_partition_id();
		DCHECK(table_id == table.tableID());
		DCHECK(partition_id == table.partitionID());
		auto key_size = table.key_size();
		auto field_size = table.field_size();

		/*
		 * The structure of a replication request: (primary key, field value,
		 * commit_tid, sync_redo).
		 * The structure of a replication response: null
		 */

		DCHECK(inputPiece.get_message_length() == MessagePiece::get_header_size() + key_size + field_size + sizeof(uint64_t) + sizeof(bool));

		auto stringPiece = inputPiece.toStringPiece();

		const void *key = stringPiece.data();
		stringPiece.remove_prefix(key_size);
		// auto valueStringPiece = stringPiece;
		// stringPiece.remove_prefix(field_size);

		uint64_t commit_ts;
		bool sync_redo = false;
		Decoder dec(stringPiece);
		dec >> commit_ts >> sync_redo;

		DCHECK(dec.size() == 0);

		// auto row = table.search(key);
		// SundialPashaHelper::replica_update(row, valueStringPiece.data(), field_size, commit_ts);

		// std::atomic<uint64_t> &tid = table.search_metadata(key);

		// uint64_t last_tid = SundialPashaHelper::lock(tid);
		// DCHECK(last_tid < commit_tid);
		// table.deserialize_value(key, valueStringPiece);
		// SundialPashaHelper::unlock(tid, commit_tid);

		// uint64_t lsn = 0;
		//  if (txn->get_logger()) {
		//    std::ostringstream ss;
		//    ss << commit_tid << std::string((const char *)key, key_size) << std::string(valueStringPiece.data(), field_size);
		//    auto output = ss.str();
		//    txn->get_logger()->write(output.c_str(), output.size(), sync_redo);
		//  }

		// if (txn->get_logger() && sync_redo) {
		//   txn->get_logger()->sync(lsn, [&](){ txn->remote_request_handler(); });
		// }

		// prepare response message header
		auto message_size = MessagePiece::get_header_size();
		auto message_piece_header = MessagePiece::construct_message_piece_header(static_cast<uint32_t>(SundialPashaMessage::REPLICATION_RESPONSE),
											 message_size, table_id, partition_id);

		star::Encoder encoder(responseMessage.data);
		encoder << message_piece_header;
		responseMessage.flush();
		responseMessage.set_gen_time(Time::now());
	}

	static void replication_response_handler(MessagePiece inputPiece, Message &responseMessage, ITable &table, Transaction *txn)
	{
		DCHECK(inputPiece.get_message_type() == static_cast<uint32_t>(SundialPashaMessage::REPLICATION_RESPONSE));
		auto table_id = inputPiece.get_table_id();
		auto partition_id = inputPiece.get_partition_id();
		DCHECK(table_id == table.tableID());
		DCHECK(partition_id == table.partitionID());
		auto key_size = table.key_size();

		/*
		 * The structure of a replication response: ()
		 */

		txn->pendingResponses--;
		txn->network_size += inputPiece.get_message_length();
	}

	static void release_lock_request_handler(MessagePiece inputPiece, Message &responseMessage, ITable &table, Transaction *txn)
	{
		DCHECK(inputPiece.get_message_type() == static_cast<uint32_t>(SundialPashaMessage::RELEASE_LOCK_REQUEST));
		auto table_id = inputPiece.get_table_id();
		auto partition_id = inputPiece.get_partition_id();
		DCHECK(table_id == table.tableID());
		DCHECK(partition_id == table.partitionID());
		auto key_size = table.key_size();

		/*
		 * The structure of a release lock request: (primary key, commit tid)
		 * The structure of a write response: null
		 */

		DCHECK(inputPiece.get_message_length() == MessagePiece::get_header_size() + key_size + sizeof(uint64_t));

		auto stringPiece = inputPiece.toStringPiece();

		const void *key = stringPiece.data();
		stringPiece.remove_prefix(key_size);

		uint64_t commit_tid;
		Decoder dec(stringPiece);
		dec >> commit_tid;
		DCHECK(dec.size() == 0);

		std::atomic<uint64_t> &tid = table.search_metadata(key);
		SundialPashaHelper::unlock(tid, commit_tid);
	}

	static void read_request_handler(MessagePiece inputPiece, Message &responseMessage, ITable &table, Transaction *txn)
	{
		DCHECK(inputPiece.get_message_type() == static_cast<uint32_t>(SundialPashaMessage::READ_REQUEST));
		auto table_id = inputPiece.get_table_id();
		auto partition_id = inputPiece.get_partition_id();
		DCHECK(table_id == table.tableID());
		DCHECK(partition_id == table.partitionID());
		auto key_size = table.key_size();
		auto value_size = table.value_size();

		/*
		 * The structure of a read request: (primary key, read key offset)
		 * The structure of a read response: (value, rts, wts, read key offset)
		 */

		auto stringPiece = inputPiece.toStringPiece();
		bool write_lock;
		uint32_t key_offset;
		uint64_t transaction_id;
		uint64_t rts, wts;

		DCHECK(inputPiece.get_message_length() ==
		       MessagePiece::get_header_size() + key_size + sizeof(transaction_id) + sizeof(write_lock) + sizeof(key_offset));

		// get row and offset
		const void *key = stringPiece.data();
		auto row = table.search(key);

		stringPiece.remove_prefix(key_size);
		star::Decoder dec(stringPiece);
		dec >> transaction_id >> write_lock >> key_offset;

		DCHECK(dec.size() == 0);

		// prepare response message header
		auto message_size = MessagePiece::get_header_size() + value_size + sizeof(bool) + sizeof(bool) + sizeof(rts) + sizeof(wts) + sizeof(key_offset);
		auto message_piece_header = MessagePiece::construct_message_piece_header(static_cast<uint32_t>(SundialPashaMessage::READ_RESPONSE), message_size,
											 table_id, partition_id);

		star::Encoder encoder(responseMessage.data);
		encoder << message_piece_header;

		// reserve size for read
		bool success = true;
		std::pair<uint64_t, uint64_t> rwts;
		if (write_lock) {
			success = SundialPashaHelper::write_lock(row, rwts, transaction_id);
		}

		responseMessage.data.append(value_size, 0);
		void *dest = &responseMessage.data[0] + responseMessage.data.size() - value_size;
		// read to message buffer
		auto read_rwts = SundialPashaHelper::read(row, dest, value_size);
		if (success && write_lock) {
			DCHECK(read_rwts == rwts);
		}
		encoder << success << write_lock << read_rwts.first << read_rwts.second << key_offset;

		responseMessage.flush();
	}

	static void read_response_handler(MessagePiece inputPiece, Message &responseMessage, ITable &table, Transaction *txn)
	{
		DCHECK(inputPiece.get_message_type() == static_cast<uint32_t>(SundialPashaMessage::READ_RESPONSE));
		auto table_id = inputPiece.get_table_id();
		auto partition_id = inputPiece.get_partition_id();
		DCHECK(table_id == table.tableID());
		DCHECK(partition_id == table.partitionID());
		auto key_size = table.key_size();
		auto value_size = table.value_size();

		/*
		 * The structure of a read request: (primary key, transaction_id, write-lock key offset in the write-set)
		 * The structure of a read response: (success, rts, wts, write-lock key offset in the write-set)
		 */

		auto stringPiece = inputPiece.toStringPiece();
		uint32_t key_offset;
		uint64_t rts, wts;
		bool write_lock, success;

		DCHECK(inputPiece.get_message_length() ==
		       MessagePiece::get_header_size() + value_size + sizeof(write_lock) + sizeof(success) + sizeof(wts) + sizeof(rts) + sizeof(key_offset));

		stringPiece.remove_prefix(value_size);
		Decoder dec(stringPiece);
		dec >> success >> write_lock >> wts >> rts >> key_offset;

		SundialPashaRWKey &readKey = txn->readSet[key_offset];
		dec = Decoder(inputPiece.toStringPiece());
		dec.read_n_bytes(readKey.get_value(), value_size);

		DCHECK(dec.size() == sizeof(success) + sizeof(write_lock) + sizeof(wts) + sizeof(rts) + sizeof(key_offset));
		txn->pendingResponses--;

		if (write_lock == false) {
			DCHECK(success == true);
			readKey.set_wts(wts);
			readKey.set_rts(rts);
			txn->commit_ts = std::max(txn->commit_ts, wts);
		} else {
			if (success == false) {
				txn->abort_lock = true;
			} else {
				readKey.set_wts(wts);
				readKey.set_rts(rts);
				readKey.set_write_lock_bit();
				txn->commit_ts = std::max(txn->commit_ts, wts);
			}
		}
	}

	static void write_lock_request_handler(MessagePiece inputPiece, Message &responseMessage, ITable &table, Transaction *txn)
	{
		DCHECK(inputPiece.get_message_type() == static_cast<uint32_t>(SundialPashaMessage::WRITE_LOCK_REQUEST));
		auto table_id = inputPiece.get_table_id();
		auto partition_id = inputPiece.get_partition_id();
		DCHECK(table_id == table.tableID());
		DCHECK(partition_id == table.partitionID());
		auto key_size = table.key_size();
		auto value_size = table.value_size();

		/*
		 * The structure of a read request: (primary key, transaction_id, write-lock key offset in the write-set)
		 * The structure of a read response: (success, rts, wts, write-lock key offset in the write-set)
		 */

		auto stringPiece = inputPiece.toStringPiece();
		uint32_t key_offset;
		uint64_t rts, wts;
		uint64_t transaction_id;
		bool success = false;

		DCHECK(inputPiece.get_message_length() == MessagePiece::get_header_size() + key_size + sizeof(transaction_id) + sizeof(key_offset));

		// get row and offset
		const void *key = stringPiece.data();
		auto row = table.search(key);

		stringPiece.remove_prefix(key_size);
		star::Decoder dec(stringPiece);
		dec >> transaction_id >> key_offset;

		DCHECK(dec.size() == 0);

		// prepare response message header
		auto message_size = MessagePiece::get_header_size() + sizeof(success) + sizeof(rts) + sizeof(wts) + sizeof(key_offset);
		auto message_piece_header = MessagePiece::construct_message_piece_header(static_cast<uint32_t>(SundialPashaMessage::WRITE_LOCK_RESPONSE),
											 message_size, table_id, partition_id);

		star::Encoder encoder(responseMessage.data);
		encoder << message_piece_header;

		std::pair<uint64_t, uint64_t> rwts;
		success = SundialPashaHelper::write_lock(row, rwts, transaction_id);

		encoder << success << rwts.first << rwts.second << key_offset;
		responseMessage.flush();
	}

	static void write_lock_response_handler(MessagePiece inputPiece, Message &responseMessage, ITable &table, Transaction *txn)
	{
		DCHECK(inputPiece.get_message_type() == static_cast<uint32_t>(SundialPashaMessage::WRITE_LOCK_RESPONSE));
		auto table_id = inputPiece.get_table_id();
		auto partition_id = inputPiece.get_partition_id();
		DCHECK(table_id == table.tableID());
		DCHECK(partition_id == table.partitionID());
		auto key_size = table.key_size();
		auto value_size = table.value_size();

		/*
		 * The structure of a read request: (primary key, transaction_id, write-lock key offset in the write-set)
		 * The structure of a read response: (success, rts, wts, write-lock key offset in the write-set)
		 */

		auto stringPiece = inputPiece.toStringPiece();
		uint32_t key_offset;
		uint64_t rts, wts;
		bool success;

		DCHECK(inputPiece.get_message_length() == MessagePiece::get_header_size() + sizeof(success) + sizeof(wts) + sizeof(rts) + sizeof(key_offset));

		// get row and offset
		star::Decoder dec(stringPiece);
		dec >> success >> wts >> rts >> key_offset;

		DCHECK(dec.size() == 0);
		txn->pendingResponses--;

		auto read_set_offset = txn->writeSet[key_offset].get_read_set_pos();
		if (success == false || (read_set_offset != -1 && txn->readSet[read_set_offset].get_wts() != wts)) {
			txn->abort_lock = true;
		} else {
			txn->commit_ts = std::max(txn->commit_ts, rts + 1);
			txn->writeSet[key_offset].set_write_lock_bit();
		}
	}

	static void renew_lease_request_handler(MessagePiece inputPiece, Message &responseMessage, ITable &table, Transaction *txn)
	{
		DCHECK(inputPiece.get_message_type() == static_cast<uint32_t>(SundialPashaMessage::RENEW_LEASE_REQUEST));
		auto table_id = inputPiece.get_table_id();
		auto partition_id = inputPiece.get_partition_id();
		DCHECK(table_id == table.tableID());
		DCHECK(partition_id == table.partitionID());
		auto key_size = table.key_size();
		auto value_size = table.value_size();

		/*
		 * The structure of a read request: (primary key, wts, commit_ts, key_offset)
		 * The structure of a read response: (success, key_offset)
		 */

		auto stringPiece = inputPiece.toStringPiece();
		uint32_t key_offset;
		uint64_t wts, commit_ts;
		bool success = false;

		DCHECK(inputPiece.get_message_length() == MessagePiece::get_header_size() + key_size + sizeof(wts) + sizeof(commit_ts) + sizeof(key_offset));

		// get row and offset
		const void *key = stringPiece.data();
		auto row = table.search(key);

		stringPiece.remove_prefix(key_size);
		star::Decoder dec(stringPiece);
		dec >> wts >> commit_ts >> key_offset;

		DCHECK(dec.size() == 0);

		// prepare response message header
		auto message_size = MessagePiece::get_header_size() + sizeof(success) + sizeof(key_offset);
		auto message_piece_header = MessagePiece::construct_message_piece_header(static_cast<uint32_t>(SundialPashaMessage::RENEW_LEASE_RESPONSE),
											 message_size, table_id, partition_id);

		star::Encoder encoder(responseMessage.data);
		encoder << message_piece_header;

		success = SundialPashaHelper::renew_lease(row, wts, commit_ts);

		encoder << success << key_offset;
		responseMessage.flush();
	}

	static void unlock_request_handler(MessagePiece inputPiece, Message &responseMessage, ITable &table, Transaction *txn)
	{
		DCHECK(inputPiece.get_message_type() == static_cast<uint32_t>(SundialPashaMessage::UNLOCK_REQUEST));
		auto table_id = inputPiece.get_table_id();
		auto partition_id = inputPiece.get_partition_id();
		DCHECK(table_id == table.tableID());
		DCHECK(partition_id == table.partitionID());
		auto key_size = table.key_size();
		auto value_size = table.value_size();

		/*
		 * The structure of a unlock request: (primary key, transaction_id, write key offset)
		 */

		auto stringPiece = inputPiece.toStringPiece();
		uint32_t key_offset;
		uint64_t transaction_id;

		DCHECK(inputPiece.get_message_length() == MessagePiece::get_header_size() + key_size + sizeof(transaction_id) + sizeof(key_offset));

		// get row and offset
		const void *key = stringPiece.data();
		auto row = table.search(key);

		stringPiece.remove_prefix(key_size);
		star::Decoder dec(stringPiece);
		dec >> transaction_id >> key_offset;

		DCHECK(dec.size() == 0);

		SundialPashaHelper::unlock(row, transaction_id);
	}

	static void update_unlock_request_handler(MessagePiece inputPiece, Message &responseMessage, ITable &table, Transaction *txn)
	{
		DCHECK(inputPiece.get_message_type() == static_cast<uint32_t>(SundialPashaMessage::UPDATE_UNLOCK_REQUEST));
		auto table_id = inputPiece.get_table_id();
		auto partition_id = inputPiece.get_partition_id();
		DCHECK(table_id == table.tableID());
		DCHECK(partition_id == table.partitionID());
		auto key_size = table.key_size();
		auto value_size = table.value_size();

		/*
		 * The structure of a unlock request: (primary key, value, transaction_id, commit_ts, write key offset)
		 */

		auto stringPiece = inputPiece.toStringPiece();
		uint32_t key_offset;
		uint64_t transaction_id;
		uint64_t commit_ts;

		DCHECK(inputPiece.get_message_length() ==
		       MessagePiece::get_header_size() + key_size + value_size + sizeof(transaction_id) + sizeof(commit_ts) + sizeof(key_offset));

		// get row and offset
		const void *key = stringPiece.data();
		auto row = table.search(key);

		stringPiece.remove_prefix(key_size);
		const void *value = stringPiece.data();
		stringPiece.remove_prefix(value_size);

		star::Decoder dec(stringPiece);
		dec >> transaction_id >> commit_ts >> key_offset;

		DCHECK(dec.size() == 0);

		SundialPashaHelper::update_unlock(row, value, value_size, commit_ts, transaction_id);
	}

	static std::vector<std::function<void(MessagePiece, Message &, ITable &, Transaction *)> > get_message_handlers()
	{
		std::vector<std::function<void(MessagePiece, Message &, ITable &, Transaction *)> > v;
		v.resize(static_cast<int>(ControlMessage::NFIELDS));
		v.push_back(search_request_handler);
		v.push_back(search_response_handler);
		v.push_back(lock_request_handler);
		v.push_back(lock_response_handler);
		v.push_back(read_validation_request_handler);
		v.push_back(read_validation_response_handler);
		v.push_back(read_validation_and_redo_request_handler);
		v.push_back(read_validation_and_redo_response_handler);
		v.push_back(abort_request_handler);
		v.push_back(write_request_handler);
		v.push_back(write_response_handler);
		v.push_back(replication_request_handler);
		v.push_back(replication_response_handler);
		v.push_back(release_lock_request_handler);
		v.push_back(read_request_handler);
		v.push_back(read_response_handler);
		v.push_back(write_lock_request_handler);
		v.push_back(write_lock_response_handler);
		v.push_back(unlock_request_handler);
		return v;
	}
};
} // namespace star

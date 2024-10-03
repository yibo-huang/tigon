//
// Created by Xinjing on 9/12/21.
//

#pragma once

#include "common/Encoder.h"
#include "common/Message.h"
#include "common/MessagePiece.h"
#include "core/ControlMessage.h"
#include "core/Table.h"

#include "protocol/H-Store/HStoreHelper.h"
#include "protocol/TwoPL/TwoPLRWKey.h"
#include "protocol/H-Store/HStoreTransaction.h"

namespace star
{

struct TxnCommandBase {
	uint64_t tid;
	int64_t position_in_log;
	void *txn;
	uint64_t last_writer = std::numeric_limits<uint64_t>::max();
	int partition_id;
	int granule_id = -1;
	bool is_coordinator;
	bool is_mp;
	bool write_lock = false;
	bool is_cow_begin = false;
	bool is_cow_end = false;
	bool is_cow_cmd_processed = false;
};
struct TxnCommand : public TxnCommandBase {
	std::string command_data;
};

enum class HStoreMessage {
	ABORT_REQUEST,
	WRITE_REQUEST,
	WRITE_RESPONSE,
	REPLICATION_REQUEST,
	REPLICATION_RESPONSE,
	RELEASE_READ_LOCK_REQUEST,
	RELEASE_READ_LOCK_RESPONSE,
	RELEASE_WRITE_LOCK_REQUEST,
	RELEASE_WRITE_LOCK_RESPONSE,
	MASTER_LOCK_PARTITION_REQUEST,
	MASTER_LOCK_PARTITION_RESPONSE,
	MASTER_UNLOCK_PARTITION_REQUEST,
	MASTER_UNLOCK_PARTITION_RESPONSE,
	COMMAND_REPLICATION_REQUEST,
	COMMAND_REPLICATION_RESPONSE,
	COMMAND_REPLICATION_SP_REQUEST,
	COMMAND_REPLICATION_SP_RESPONSE,
	ACQUIRE_LOCK_AND_READ_REQUEST,
	ACQUIRE_LOCK_AND_READ_RESPONSE,
	WRITE_BACK_REQUEST,
	WRITE_BACK_RESPONSE,
	RELEASE_LOCK_REQUEST,
	RELEASE_LOCK_RESPONSE,
	PERSIST_CMD_BUFFER_REQUEST,
	PERSIST_CMD_BUFFER_RESPONSE,
	GET_REPLAYED_LOG_POSITION_REQUEST,
	GET_REPLAYED_LOG_POSITION_RESPONSE,
	RTT_REQUEST,
	RTT_RESPONSE,
	CHECKPOINT_INST,
	WRITE_PARTICIPANT_COMMAND_REQUEST,
	NFIELDS
};

class HStoreMessageFactory {
    public:
	static std::size_t new_checkpoint_instruction_message(Message &message, int inst, int ith_replica, int cluster_worker_id)
	{
		/*
		 * The structure of a execute stage change request: ()
		 */

		auto message_size = MessagePiece::get_header_size() + sizeof(cluster_worker_id) + sizeof(ith_replica) + sizeof(inst);
		auto message_piece_header =
			MessagePiece::construct_message_piece_header(static_cast<uint32_t>(HStoreMessage::CHECKPOINT_INST), message_size, 0, 0);

		Encoder encoder(message.data);
		encoder << message_piece_header << cluster_worker_id << ith_replica << inst;
		message.set_is_replica(ith_replica > 0);
		message.flush();
		message.set_message_gen_time(0);
		message.set_gen_time(Time::now());
		return message_size;
	}

	static std::size_t new_rtt_message(Message &message, int ith_replica, int cluster_worker_id)
	{
		/*
		 * The structure of a persist command buffer request: ()
		 */

		auto message_size = MessagePiece::get_header_size() + sizeof(cluster_worker_id) + sizeof(ith_replica);
		auto message_piece_header = MessagePiece::construct_message_piece_header(static_cast<uint32_t>(HStoreMessage::RTT_REQUEST), message_size, 0, 0);

		Encoder encoder(message.data);
		encoder << message_piece_header << cluster_worker_id << ith_replica;
		message.set_is_replica(ith_replica > 0);
		message.flush();
		message.set_message_gen_time(0);
		message.set_gen_time(Time::now());
		return message_size;
	}

	static std::size_t new_get_replayed_log_posistion_message(Message &message, int64_t desired_posisiton, int ith_replica, int cluster_worker_id)
	{
		/*
		 * The structure of a persist command buffer request: ()
		 */

		auto message_size = MessagePiece::get_header_size() + sizeof(cluster_worker_id) + sizeof(ith_replica) + sizeof(desired_posisiton);
		auto message_piece_header = MessagePiece::construct_message_piece_header(
			static_cast<uint32_t>(HStoreMessage::GET_REPLAYED_LOG_POSITION_REQUEST), message_size, 0, 0);

		Encoder encoder(message.data);
		encoder << message_piece_header << desired_posisiton << cluster_worker_id << ith_replica;
		message.set_is_replica(ith_replica > 0);
		message.flush();
		message.set_gen_time(Time::now());
		message.set_message_gen_time(0);
		return message_size;
	}

	static std::size_t new_persist_cmd_buffer_message(Message &message, int ith_replica, int cluster_worker_id)
	{
		/*
		 * The structure of a persist command buffer request: ()
		 */

		auto message_size = MessagePiece::get_header_size() + sizeof(cluster_worker_id) + sizeof(ith_replica);
		auto message_piece_header =
			MessagePiece::construct_message_piece_header(static_cast<uint32_t>(HStoreMessage::PERSIST_CMD_BUFFER_REQUEST), message_size, 0, 0);

		Encoder encoder(message.data);
		encoder << message_piece_header << cluster_worker_id << ith_replica;
		message.set_is_replica(ith_replica > 0);
		message.flush();
		message.set_gen_time(Time::now());
		return message_size;
	}

	static std::size_t new_write_participant_command_message(Message &message, ITable &table, uint32_t this_worker_id, uint32_t granule_id,
								 uint64_t requested_last_writer, bool write_lock, std::size_t ith_replica,
								 const TxnCommand &txn_cmd)
	{
		auto message_size = MessagePiece::get_header_size() + sizeof(granule_id) + sizeof(uint32_t) + sizeof(requested_last_writer) +
				    sizeof(write_lock) + sizeof(ith_replica);
		message_size += sizeof(txn_cmd.is_mp) + sizeof(txn_cmd.command_data.size()) + txn_cmd.command_data.size() + sizeof(txn_cmd.tid) +
				sizeof(txn_cmd.partition_id);
		auto message_piece_header =
			MessagePiece::construct_message_piece_header(static_cast<uint32_t>(HStoreMessage::WRITE_PARTICIPANT_COMMAND_REQUEST), message_size,
								     table.tableID(), table.partitionID(), granule_id);

		Encoder encoder(message.data);
		encoder << message_piece_header;
		encoder << this_worker_id;
		encoder << granule_id;
		encoder << requested_last_writer;
		encoder << write_lock;
		encoder << ith_replica;
		encoder << txn_cmd.partition_id;
		encoder << txn_cmd.tid;
		encoder << txn_cmd.is_mp;
		encoder << txn_cmd.command_data.size();
		if (txn_cmd.command_data.size())
			encoder.write_n_bytes(txn_cmd.command_data.data(), txn_cmd.command_data.size());
		message.set_is_replica(ith_replica > 0);
		message.flush();
		message.set_gen_time(Time::now());
		message.set_message_gen_time(0);
		return message_size;
	}

	static std::size_t new_release_lock_message(Message &message, ITable &table, uint32_t this_worker_id, uint32_t granule_id,
						    uint64_t requested_last_writer, bool write_lock, bool sync, std::size_t ith_replica, bool write_cmd_buffer,
						    const TxnCommand &txn_cmd)
	{
		auto message_size = MessagePiece::get_header_size() + sizeof(granule_id) + sizeof(uint32_t) + sizeof(requested_last_writer) +
				    sizeof(write_lock) + sizeof(bool) + sizeof(std::size_t) + sizeof(write_cmd_buffer);
		if (write_cmd_buffer)
			message_size += sizeof(txn_cmd.is_mp) + sizeof(txn_cmd.command_data.size()) + txn_cmd.command_data.size() + sizeof(txn_cmd.tid) +
					sizeof(txn_cmd.partition_id);
		auto message_piece_header = MessagePiece::construct_message_piece_header(static_cast<uint32_t>(HStoreMessage::RELEASE_LOCK_REQUEST),
											 message_size, table.tableID(), table.partitionID(), granule_id);

		Encoder encoder(message.data);
		encoder << message_piece_header;
		encoder << this_worker_id;
		encoder << granule_id;
		encoder << requested_last_writer;
		encoder << write_lock;
		encoder << sync;
		encoder << ith_replica;
		encoder << write_cmd_buffer;
		if (write_cmd_buffer) {
			encoder << txn_cmd.partition_id;
			encoder << txn_cmd.tid;
			encoder << txn_cmd.is_mp;
			encoder << txn_cmd.command_data.size();
			if (txn_cmd.command_data.size())
				encoder.write_n_bytes(txn_cmd.command_data.data(), txn_cmd.command_data.size());
		}
		message.set_is_replica(ith_replica > 0);
		message.flush();
		message.set_gen_time(Time::now());
		message.set_message_gen_time(0);
		return message_size;
	}

	static std::size_t new_command_replication_response_message(Message &message)
	{
		auto message_size = MessagePiece::get_header_size();

		auto message_piece_header =
			MessagePiece::construct_message_piece_header(static_cast<uint32_t>(HStoreMessage::COMMAND_REPLICATION_RESPONSE), message_size, 0, 0);

		star::Encoder encoder(message.data);
		encoder << message_piece_header;

		message.flush();
		message.set_gen_time(Time::now());
		return message_size;
	}

	static std::size_t new_command_replication(Message &message, std::size_t ith_replica, const std::string &data, int this_cluster_worker_id,
						   bool persist_cmd_buffer)
	{
		auto message_size =
			MessagePiece::get_header_size() + sizeof(std::size_t) + data.size() + sizeof(this_cluster_worker_id) + sizeof(persist_cmd_buffer);
		auto message_piece_header =
			MessagePiece::construct_message_piece_header(static_cast<uint32_t>(HStoreMessage::COMMAND_REPLICATION_REQUEST), message_size, 0, 0);
		Encoder encoder(message.data);
		encoder << message_piece_header << ith_replica << this_cluster_worker_id << persist_cmd_buffer;
		encoder.write_n_bytes(data.c_str(), data.size());
		message.set_is_replica(ith_replica > 0);
		message.flush();
		message.set_message_gen_time(0);
		message.set_gen_time(Time::now());
		return message_size;
	}

	static std::size_t new_acquire_lock_and_read_message(Message &message, ITable &table, const void *key, uint32_t key_offset, uint32_t this_worker_id,
							     uint32_t granule_id, uint64_t requested_last_writer,
							     bool write_lock, // If this lock request is write lock
							     bool request_lock, // If this lock request needs to be processed.
							     std::size_t ith_replica, uint64_t tries)
	{
		/*
		 * The structure of a partition lock request: (primary key, key offset, remote_worker_id)
		 */

		auto key_size = table.key_size();

		auto message_size = MessagePiece::get_header_size() + key_size + sizeof(key_offset) + sizeof(granule_id) + sizeof(requested_last_writer) +
				    sizeof(write_lock) + sizeof(request_lock) + sizeof(uint32_t) + sizeof(std::size_t) + sizeof(uint64_t);
		auto message_piece_header = MessagePiece::construct_message_piece_header(static_cast<uint32_t>(HStoreMessage::ACQUIRE_LOCK_AND_READ_REQUEST),
											 message_size, table.tableID(), table.partitionID(), granule_id);

		// LOG(INFO) << "this_cluster_worker_id "<< this_worker_id << " new_acquire_partition_lock_and_read_message message on partition "
		//           << table.partitionID() << " of " << ith_replica << " replica";
		Encoder encoder(message.data);
		encoder << message_piece_header;
		encoder.write_n_bytes(key, key_size);
		encoder << key_offset;
		encoder << this_worker_id;
		encoder << granule_id;
		encoder << requested_last_writer;
		encoder << write_lock;
		encoder << request_lock;
		encoder << ith_replica;
		encoder << tries;
		message.set_is_replica(ith_replica > 0);
		message.set_message_gen_time(0);
		message.flush();
		message.set_gen_time(Time::now());
		return message_size;
	}

	static std::size_t new_write_back_message(Message &message, ITable &table, const void *key, const void *value, uint32_t this_worker_id,
						  uint32_t granule_id, uint64_t commit_tid, std::size_t ith_replica, bool persist_commit_record = false)
	{
		/*
		 * The structure of a write request: (request_remote_worker, nowrite, primary key, field value)
		 */

		auto key_size = table.key_size();
		auto field_size = table.field_size();

		auto message_size = MessagePiece::get_header_size() + sizeof(granule_id) + sizeof(uint32_t) + sizeof(std::size_t) + key_size + field_size +
				    sizeof(commit_tid) + sizeof(persist_commit_record);
		auto message_piece_header = MessagePiece::construct_message_piece_header(static_cast<uint32_t>(HStoreMessage::WRITE_BACK_REQUEST), message_size,
											 table.tableID(), table.partitionID(), granule_id);

		Encoder encoder(message.data);
		encoder << message_piece_header << commit_tid << persist_commit_record;
		encoder << this_worker_id;
		encoder << granule_id;
		encoder << ith_replica;
		encoder.write_n_bytes(key, key_size);
		table.serialize_value(encoder, value);
		message.set_is_replica(ith_replica > 0);
		message.flush();
		message.set_gen_time(Time::now());
		message.set_message_gen_time(0);
		return message_size;
	}

	static std::size_t new_abort_message(Message &message, ITable &table, const void *key, bool write_lock)
	{
		/*
		 * The structure of an abort request: (primary key, wrtie lock)
		 */

		auto key_size = table.key_size();

		auto message_size = MessagePiece::get_header_size() + key_size + sizeof(bool);
		auto message_piece_header = MessagePiece::construct_message_piece_header(static_cast<uint32_t>(HStoreMessage::ABORT_REQUEST), message_size,
											 table.tableID(), table.partitionID());

		Encoder encoder(message.data);
		encoder << message_piece_header;
		encoder.write_n_bytes(key, key_size);
		encoder << write_lock;
		message.flush();
		message.set_gen_time(Time::now());
		return message_size;
	}

	static std::size_t new_write_message(Message &message, ITable &table, const void *key, const void *value)
	{
		/*
		 * The structure of a write request: (primary key, field value)
		 */

		auto key_size = table.key_size();
		auto field_size = table.field_size();

		auto message_size = MessagePiece::get_header_size() + key_size + field_size;
		auto message_piece_header = MessagePiece::construct_message_piece_header(static_cast<uint32_t>(HStoreMessage::WRITE_REQUEST), message_size,
											 table.tableID(), table.partitionID());

		Encoder encoder(message.data);
		encoder << message_piece_header;
		encoder.write_n_bytes(key, key_size);
		table.serialize_value(encoder, value);
		message.flush();
		message.set_gen_time(Time::now());
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
		auto message_piece_header = MessagePiece::construct_message_piece_header(static_cast<uint32_t>(HStoreMessage::REPLICATION_REQUEST),
											 message_size, table.tableID(), table.partitionID());

		Encoder encoder(message.data);
		encoder << message_piece_header;
		encoder.write_n_bytes(key, key_size);
		table.serialize_value(encoder, value);
		encoder << commit_tid;
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
		auto message_piece_header = MessagePiece::construct_message_piece_header(static_cast<uint32_t>(HStoreMessage::RELEASE_READ_LOCK_REQUEST),
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
		auto message_piece_header = MessagePiece::construct_message_piece_header(static_cast<uint32_t>(HStoreMessage::RELEASE_WRITE_LOCK_REQUEST),
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

class HStoreMessageHandler {
	using Transaction = HStoreTransaction;

    public:
	static void abort_request_handler(MessagePiece inputPiece, Message &responseMessage, ITable &table, Transaction *txn)
	{
		DCHECK(inputPiece.get_message_type() == static_cast<uint32_t>(HStoreMessage::ABORT_REQUEST));
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

		std::atomic<uint64_t> &tid = *table.search_metadata(key);

		if (write_lock) {
			TwoPLHelper::write_lock_release(tid);
		} else {
			TwoPLHelper::read_lock_release(tid);
		}
	}

	static void write_request_handler(MessagePiece inputPiece, Message &responseMessage, ITable &table, Transaction *txn)
	{
		DCHECK(inputPiece.get_message_type() == static_cast<uint32_t>(HStoreMessage::WRITE_REQUEST));
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

		auto stringPiece = inputPiece.toStringPiece();

		const void *key = stringPiece.data();
		stringPiece.remove_prefix(key_size);

		table.deserialize_value(key, stringPiece);

		// prepare response message header
		auto message_size = MessagePiece::get_header_size();
		auto message_piece_header = MessagePiece::construct_message_piece_header(static_cast<uint32_t>(HStoreMessage::WRITE_RESPONSE), message_size,
											 table_id, partition_id);

		star::Encoder encoder(responseMessage.data);
		encoder << message_piece_header;
		responseMessage.flush();
	}

	static void write_response_handler(MessagePiece inputPiece, Message &responseMessage, ITable &table, Transaction *txn)
	{
		DCHECK(inputPiece.get_message_type() == static_cast<uint32_t>(HStoreMessage::WRITE_RESPONSE));
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

	static void replication_request_handler(MessagePiece inputPiece, Message &responseMessage, ITable &table, Transaction *txn)
	{
		DCHECK(inputPiece.get_message_type() == static_cast<uint32_t>(HStoreMessage::REPLICATION_REQUEST));
		auto table_id = inputPiece.get_table_id();
		auto partition_id = inputPiece.get_partition_id();
		DCHECK(table_id == table.tableID());
		DCHECK(partition_id == table.partitionID());
		auto key_size = table.key_size();
		auto field_size = table.field_size();

		/*
		 * The structure of a replication request: (primary key, field value, commit
		 * tid) The structure of a replication response: ()
		 */

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

		uint64_t last_tid = TwoPLHelper::write_lock(tid);
		DCHECK(last_tid < commit_tid);
		table.deserialize_value(key, valueStringPiece);
		TwoPLHelper::write_lock_release(tid, commit_tid);

		// prepare response message header
		auto message_size = MessagePiece::get_header_size();
		auto message_piece_header = MessagePiece::construct_message_piece_header(static_cast<uint32_t>(HStoreMessage::REPLICATION_RESPONSE),
											 message_size, table_id, partition_id);

		star::Encoder encoder(responseMessage.data);
		encoder << message_piece_header;
		responseMessage.flush();
	}

	static void replication_response_handler(MessagePiece inputPiece, Message &responseMessage, ITable &table, Transaction *txn)
	{
		DCHECK(inputPiece.get_message_type() == static_cast<uint32_t>(HStoreMessage::REPLICATION_RESPONSE));
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

	static void release_read_lock_request_handler(MessagePiece inputPiece, Message &responseMessage, ITable &table, Transaction *txn)
	{
		DCHECK(inputPiece.get_message_type() == static_cast<uint32_t>(HStoreMessage::RELEASE_READ_LOCK_REQUEST));
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

		std::atomic<uint64_t> &tid = *table.search_metadata(key);

		TwoPLHelper::read_lock_release(tid);

		// prepare response message header
		auto message_size = MessagePiece::get_header_size();
		auto message_piece_header = MessagePiece::construct_message_piece_header(static_cast<uint32_t>(HStoreMessage::RELEASE_READ_LOCK_RESPONSE),
											 message_size, table_id, partition_id);

		star::Encoder encoder(responseMessage.data);
		encoder << message_piece_header;
		responseMessage.flush();
	}

	static void release_read_lock_response_handler(MessagePiece inputPiece, Message &responseMessage, ITable &table, Transaction *txn)
	{
		DCHECK(inputPiece.get_message_type() == static_cast<uint32_t>(HStoreMessage::RELEASE_READ_LOCK_RESPONSE));
		auto table_id = inputPiece.get_table_id();
		auto partition_id = inputPiece.get_partition_id();
		DCHECK(table_id == table.tableID());
		DCHECK(partition_id == table.partitionID());
		auto key_size = table.key_size();

		/*
		 * The structure of a release read lock response: ()
		 */

		txn->pendingResponses--;
		txn->network_size += inputPiece.get_message_length();
	}

	static void release_write_lock_request_handler(MessagePiece inputPiece, Message &responseMessage, ITable &table, Transaction *txn)
	{
		DCHECK(inputPiece.get_message_type() == static_cast<uint32_t>(HStoreMessage::RELEASE_WRITE_LOCK_REQUEST));
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

		std::atomic<uint64_t> &tid = *table.search_metadata(key);
		TwoPLHelper::write_lock_release(tid, commit_tid);

		// prepare response message header
		auto message_size = MessagePiece::get_header_size();
		auto message_piece_header = MessagePiece::construct_message_piece_header(static_cast<uint32_t>(HStoreMessage::RELEASE_WRITE_LOCK_RESPONSE),
											 message_size, table_id, partition_id);

		star::Encoder encoder(responseMessage.data);
		encoder << message_piece_header;
		responseMessage.flush();
	}

	static void release_write_lock_response_handler(MessagePiece inputPiece, Message &responseMessage, ITable &table, Transaction *txn)
	{
		DCHECK(inputPiece.get_message_type() == static_cast<uint32_t>(HStoreMessage::RELEASE_WRITE_LOCK_RESPONSE));
		auto table_id = inputPiece.get_table_id();
		auto partition_id = inputPiece.get_partition_id();
		DCHECK(table_id == table.tableID());
		DCHECK(partition_id == table.partitionID());
		auto key_size = table.key_size();

		/*
		 * The structure of a release write lock response: ()
		 */

		txn->pendingResponses--;
		txn->network_size += inputPiece.get_message_length();
	}

    public:
	static std::vector<std::function<void(MessagePiece, Message &, ITable &, Transaction *)> > get_message_handlers()
	{
		std::vector<std::function<void(MessagePiece, Message &, ITable &, Transaction *)> > v;
		v.resize(static_cast<int>(ControlMessage::NFIELDS));
		v.push_back(abort_request_handler);
		v.push_back(write_request_handler);
		v.push_back(write_response_handler);
		v.push_back(replication_request_handler);
		v.push_back(replication_response_handler);
		v.push_back(release_read_lock_request_handler);
		v.push_back(release_read_lock_response_handler);
		v.push_back(release_write_lock_request_handler);
		v.push_back(release_write_lock_response_handler);
		return v;
	}
};

} // namespace star

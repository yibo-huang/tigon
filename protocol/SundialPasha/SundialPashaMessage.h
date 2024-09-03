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
	DATA_MIGRATION_REQUEST = static_cast<int>(ControlMessage::NFIELDS),
        DATA_MIGRATION_RESPONSE,
        REPLICATION_REQUEST,
	REPLICATION_RESPONSE,
	NFIELDS
};

class SundialPashaMessageFactory {
    public:
	static std::size_t new_data_migration_message(Message &message, ITable &table, const void *key, uint64_t transaction_id, bool write_lock, uint32_t key_offset)
	{
		/*
		 * The structure of a read request: (primary key, write_lock, read key offset)
		 */

		auto key_size = table.key_size();

		auto message_size = MessagePiece::get_header_size() + key_size + sizeof(transaction_id) + sizeof(key_offset);
		auto message_piece_header = MessagePiece::construct_message_piece_header(static_cast<uint32_t>(SundialPashaMessage::DATA_MIGRATION_REQUEST),
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
};

class SundialPashaMessageHandler {
	using Transaction = SundialPashaTransaction;

    public:
	static void data_migration_request_handler(MessagePiece inputPiece, Message &responseMessage, ITable &table, Transaction *txn)
	{
		DCHECK(inputPiece.get_message_type() == static_cast<uint32_t>(SundialPashaMessage::DATA_MIGRATION_REQUEST));
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
		uint32_t key_offset;
		uint64_t transaction_id;
		uint64_t rts, wts;
                bool success = false;

		DCHECK(inputPiece.get_message_length() ==
		       MessagePiece::get_header_size() + key_size + sizeof(transaction_id) + sizeof(key_offset));

		// get row and offset
		const void *key = stringPiece.data();
		auto row = table.search(key);

		stringPiece.remove_prefix(key_size);
		star::Decoder dec(stringPiece);
		dec >> transaction_id >> key_offset;

		DCHECK(dec.size() == 0);

                // move the tuple to the shared region if it is not currently there
                success = global_helper.move_from_partition_to_shared_region(&table, table.get_plain_key(key), row);
                DCHECK(success == true);

		// prepare response message header
		auto message_size = MessagePiece::get_header_size() + sizeof(success) + sizeof(key_offset);
		auto message_piece_header = MessagePiece::construct_message_piece_header(static_cast<uint32_t>(SundialPashaMessage::DATA_MIGRATION_RESPONSE), message_size,
                                                                                         table_id, partition_id);

		star::Encoder encoder(responseMessage.data);
		encoder << message_piece_header;
                encoder << success << key_offset;
		responseMessage.flush();
	}

	static void data_migration_response_handler(MessagePiece inputPiece, Message &responseMessage, ITable &table, Transaction *txn)
	{
		DCHECK(inputPiece.get_message_type() == static_cast<uint32_t>(SundialPashaMessage::DATA_MIGRATION_RESPONSE));
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
		bool success;

		DCHECK(inputPiece.get_message_length() ==
		       MessagePiece::get_header_size() + sizeof(success) + sizeof(key_offset));

		Decoder dec(stringPiece);
		dec >> success >> key_offset;
                DCHECK(success == true);

		SundialPashaRWKey &readKey = txn->readSet[key_offset];

                // search cxl table and get the data
                char *migrated_row = global_helper.get_migrated_row(table_id, partition_id, table.get_plain_key(readKey.get_key()), false);
                CHECK(migrated_row != nullptr);

                // perform execution phase operations
                std::pair<uint64_t, uint64_t> rwts;
                if (readKey.get_write_request_bit()) {
                        DCHECK(readKey.get_local_index_read_bit() == 0);
                        success = SundialPashaHelper::remote_write_lock(migrated_row, rwts, txn->transaction_id);
                }
                auto read_rwts = SundialPashaHelper::remote_read(migrated_row, readKey.get_value(), value_size);
                readKey.set_wts(read_rwts.first);
                readKey.set_rts(read_rwts.second);
                if (readKey.get_write_request_bit()) {
                        DCHECK(readKey.get_local_index_read_bit() == 0);
                        if (success) {
                                DCHECK(rwts == read_rwts);
                                readKey.set_write_lock_bit();
                        } else {
                                txn->abort_lock = true;
                        }
                }
                // mark it as reference counted so that we know if we need to release it upon commit/abort
                readKey.set_reference_counted();

                txn->pendingResponses--;
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


	static std::vector<std::function<void(MessagePiece, Message &, ITable &, Transaction *)> > get_message_handlers()
	{
		std::vector<std::function<void(MessagePiece, Message &, ITable &, Transaction *)> > v;
		v.resize(static_cast<int>(ControlMessage::NFIELDS));
		v.push_back(data_migration_request_handler);
		v.push_back(data_migration_response_handler);
                // replication is not supported
                // v.push_back(replication_request_handler);
		// v.push_back(replication_response_handler);
		return v;
	}
};
} // namespace star

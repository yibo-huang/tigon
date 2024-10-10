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

#include "protocol/TwoPLPasha/TwoPLPashaHelper.h"
#include "protocol/TwoPLPasha/TwoPLPashaRWKey.h"
#include "protocol/TwoPLPasha/TwoPLPashaTransaction.h"

namespace star
{

enum class TwoPLPashaMessage {
	DATA_MIGRATION_REQUEST = static_cast<int>(ControlMessage::NFIELDS),
        DATA_MIGRATION_RESPONSE,
        DATA_MOVEOUT_HINT,
        REPLICATION_REQUEST,
	REPLICATION_RESPONSE,
	NFIELDS
};

class TwoPLPashaMessageFactory {
    public:
        static std::size_t new_data_migration_message(Message &message, ITable &table, const void *key, uint64_t transaction_id, uint32_t key_offset)
	{
		/*
		 * The structure of a data migration request: (primary key, transaction_id, key_offset)
		 */

		auto key_size = table.key_size();

		auto message_size = MessagePiece::get_header_size() + key_size + sizeof(transaction_id) + sizeof(key_offset);
		auto message_piece_header = MessagePiece::construct_message_piece_header(static_cast<uint32_t>(TwoPLPashaMessage::DATA_MIGRATION_REQUEST),
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

        static std::size_t new_data_move_out_hint_message(Message &message)
	{
		/*
		 * The structure of a data move out hint: ()
		 */
		auto message_size = MessagePiece::get_header_size();
		auto message_piece_header = MessagePiece::construct_message_piece_header(static_cast<uint32_t>(TwoPLPashaMessage::DATA_MOVEOUT_HINT),
                                                                                         message_size, 0, 0);

		Encoder encoder(message.data);
		encoder << message_piece_header;
		message.flush();
		message.set_gen_time(Time::now());
		return message_size;
	}

	static std::size_t new_replication_message(Message &message, ITable &table, const void *key, const void *value, uint64_t commit_tid, bool sync_redo)
	{
		CHECK(0);
	}
};

class TwoPLPashaMessageHandler {
	using Transaction = TwoPLPashaTransaction;

    public:
        static void data_migration_request_handler(MessagePiece inputPiece, Message &responseMessage, ITable &table, Transaction *txn)
	{
		DCHECK(inputPiece.get_message_type() == static_cast<uint32_t>(TwoPLPashaMessage::DATA_MIGRATION_REQUEST));
		auto table_id = inputPiece.get_table_id();
		auto partition_id = inputPiece.get_partition_id();
		DCHECK(table_id == table.tableID());
		DCHECK(partition_id == table.partitionID());
		auto key_size = table.key_size();
		auto value_size = table.value_size();

		/*
		 * The structure of a data migration request: (key, transaction_id, key_offset)
		 * The structure of a data migration response: (success, key_offset)
		 */

		auto stringPiece = inputPiece.toStringPiece();
		uint32_t key_offset;
		uint64_t transaction_id;
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
                success = migration_manager->move_row_in(&table, table.get_plain_key(key), sizeof(TwoPLPashaMetadataShared) + table.value_size(), row);
                DCHECK(success == true);

		// prepare response message header
		auto message_size = MessagePiece::get_header_size() + sizeof(success) + sizeof(key_offset);
		auto message_piece_header = MessagePiece::construct_message_piece_header(static_cast<uint32_t>(TwoPLPashaMessage::DATA_MIGRATION_RESPONSE), message_size,
                                                                                         table_id, partition_id);

		star::Encoder encoder(responseMessage.data);
		encoder << message_piece_header;
                encoder << success << key_offset;
		responseMessage.flush();

                if (migration_manager->when_to_move_out == MigrationManager::OnDemand) {
                        // before moving in the tuple, we move out tuples
                        migration_manager->move_row_out();
                }
	}

	static void data_migration_response_handler(MessagePiece inputPiece, Message &responseMessage, ITable &table, Transaction *txn)
	{
		DCHECK(inputPiece.get_message_type() == static_cast<uint32_t>(TwoPLPashaMessage::DATA_MIGRATION_RESPONSE));
		auto table_id = inputPiece.get_table_id();
		auto partition_id = inputPiece.get_partition_id();
		DCHECK(table_id == table.tableID());
		DCHECK(partition_id == table.partitionID());
		auto key_size = table.key_size();
		auto value_size = table.value_size();

		/*
		 * The structure of a data migration request: (key, transaction_id, key_offset)
		 * The structure of a data migration response: (success, key_offset)
		 */

		auto stringPiece = inputPiece.toStringPiece();
		uint32_t key_offset;
		bool success;

		DCHECK(inputPiece.get_message_length() ==
		       MessagePiece::get_header_size() + sizeof(success) + sizeof(key_offset));

		Decoder dec(stringPiece);
		dec >> success >> key_offset;
                DCHECK(success == true);

		TwoPLPashaRWKey &readKey = txn->readSet[key_offset];
                uint64_t tid = 0;

                // search cxl table and get the data
                char *migrated_row = twopl_pasha_global_helper.get_migrated_row(table_id, partition_id, table.get_plain_key(readKey.get_key()), false);
                CHECK(migrated_row != nullptr);

                // perform execution phase operations
                if (readKey.get_write_lock_request_bit()) {
                        tid = TwoPLPashaHelper::remote_write_lock(migrated_row, success);
                } else {
                        tid = TwoPLPashaHelper::remote_read_lock(migrated_row, success);
                }

                if (success) {
                        readKey.set_tid(tid);
                        CHECK(readKey.get_local_index_read_bit() == 0);
                        if (readKey.get_read_lock_request_bit()) {
                                readKey.set_read_lock_bit();
                        }
                        if (readKey.get_write_lock_request_bit()) {
                                readKey.set_write_lock_bit();
                        }
                        twopl_pasha_global_helper.remote_read(migrated_row, readKey.get_value(), value_size);
                } else {
                        txn->abort_lock = true;
                }

                // mark it as reference counted so that we know if we need to release it upon commit/abort
                readKey.set_reference_counted();

                txn->pendingResponses--;
	}

        static void data_move_out_hint_handler(MessagePiece inputPiece, Message &responseMessage, ITable &table, Transaction *txn)
	{
                migration_manager->move_row_out();
	}

	static void replication_request_handler(MessagePiece inputPiece, Message &responseMessage, ITable &table, Transaction *txn)
	{
		CHECK(0);
	}

	static void replication_response_handler(MessagePiece inputPiece, Message &responseMessage, ITable &table, Transaction *txn)
	{
		CHECK(0);
	}

    public:
	static std::vector<std::function<void(MessagePiece, Message &, ITable &, Transaction *)> > get_message_handlers()
	{
		std::vector<std::function<void(MessagePiece, Message &, ITable &, Transaction *)> > v;
		v.resize(static_cast<int>(ControlMessage::NFIELDS));
		v.push_back(data_migration_request_handler);
		v.push_back(data_migration_response_handler);
                v.push_back(data_move_out_hint_handler);
                // replication is not supported
                // v.push_back(replication_request_handler);
		// v.push_back(replication_response_handler);
		return v;
	}
};

} // namespace star

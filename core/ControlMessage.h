//
// Created by Yi Lu on 9/6/18.
//

#pragma once

#include "common/Encoder.h"
#include "common/Message.h"
#include "common/MessagePiece.h"

namespace star
{

enum class ControlMessage { STATISTICS, SIGNAL, ACK, STOP, NFIELDS };

class ControlMessageFactory {
    public:
	static std::size_t new_statistics_message(Message &message, int coordinator_id, double commit,
                uint64_t size_index_usage, uint64_t size_metadata_usage, uint64_t size_data_usgae, uint64_t size_transport_usage, uint64_t size_misc_usage, uint64_t size_hwcc_usage)
	{
		/*
		 * The structure of a statistics message: (statistics value : double)
		 *
		 */

		// the message is not associated with a table or a partition, use 0.
		auto message_size = MessagePiece::get_header_size() + sizeof(coordinator_id) + sizeof(commit) +
                        sizeof(size_index_usage) + sizeof(size_metadata_usage) + sizeof(size_data_usgae) + sizeof(size_transport_usage) + sizeof(size_misc_usage) + sizeof(size_hwcc_usage);
		auto message_piece_header = MessagePiece::construct_message_piece_header(static_cast<uint32_t>(ControlMessage::STATISTICS), message_size, 0, 0);

		Encoder encoder(message.data);
		encoder << message_piece_header;
		encoder << coordinator_id << commit << size_index_usage << size_metadata_usage << size_data_usgae << size_transport_usage << size_misc_usage << size_hwcc_usage;
		message.flush();
		message.set_gen_time(Time::now());
		return message_size;
	}

	static std::size_t new_signal_message(Message &message, uint32_t value)
	{
		/*
		 * The structure of a signal message: (signal value : uint32_t)
		 */

		// the message is not associated with a table or a partition, use 0.
		auto message_size = MessagePiece::get_header_size() + sizeof(uint32_t);
		auto message_piece_header = MessagePiece::construct_message_piece_header(static_cast<uint32_t>(ControlMessage::SIGNAL), message_size, 0, 0);

		Encoder encoder(message.data);
		encoder << message_piece_header;
		encoder << value;
		message.flush();
		message.set_gen_time(Time::now());
		return message_size;
	}

	static std::size_t new_ack_message(Message &message)
	{
		/*
		 * The structure of an ack message: ()
		 */

		auto message_size = MessagePiece::get_header_size();
		auto message_piece_header = MessagePiece::construct_message_piece_header(static_cast<uint32_t>(ControlMessage::ACK), message_size, 0, 0);
		Encoder encoder(message.data);
		encoder << message_piece_header;
		message.flush();
		message.set_gen_time(Time::now());
		return message_size;
	}

	static std::size_t new_stop_message(Message &message)
	{
		/*
		 * The structure of a stop message: ()
		 */

		auto message_size = MessagePiece::get_header_size();
		auto message_piece_header = MessagePiece::construct_message_piece_header(static_cast<uint32_t>(ControlMessage::STOP), message_size, 0, 0);
		Encoder encoder(message.data);
		encoder << message_piece_header;
		message.flush();
		message.set_gen_time(Time::now());
		return message_size;
	}
};

} // namespace star

//
// Created by Yi Lu on 8/30/18.
//

#pragma once

#include "common/Message.h"
#include "common/Socket.h"
#include "common/MPSCRingBuffer.h"

#include <glog/logging.h>

namespace star
{
class BufferedReader {
    public:
	BufferedReader(Socket &socket)
		: use_cxl_transport(false)
                , socket(&socket)
                , cxl_ringbuffer(nullptr)
		, bytes_read(0)
		, bytes_total(0)
	{
	}

        BufferedReader(MPSCRingBuffer &cxl_ringbuffer)
		: use_cxl_transport(true)
                , socket(nullptr)
                , cxl_ringbuffer(&cxl_ringbuffer)
		, bytes_read(0)
		, bytes_total(0)
	{
	}

	// BufferedReader is not copyable
	BufferedReader(const BufferedReader &) = delete;

	BufferedReader &operator=(const BufferedReader &) = delete;

	// BufferedReader is movable

	BufferedReader(BufferedReader &&that)
                : use_cxl_transport(that.use_cxl_transport)
		, socket(that.socket)
                , cxl_ringbuffer(that.cxl_ringbuffer)
		, bytes_read(that.bytes_read)
		, bytes_total(that.bytes_total)
	{
                that.use_cxl_transport = false;
		that.socket = nullptr;
                that.cxl_ringbuffer = nullptr;
		that.bytes_read = 0;
		that.bytes_total = 0;
	}

	BufferedReader &operator=(BufferedReader &&that)
	{
                use_cxl_transport = that.use_cxl_transport;
		socket = that.socket;
                cxl_ringbuffer = that.cxl_ringbuffer;
		bytes_read = that.bytes_read;
		bytes_total = that.bytes_total;

		that.use_cxl_transport = false;
		that.socket = nullptr;
                that.cxl_ringbuffer = nullptr;
		that.bytes_read = 0;
		that.bytes_total = 0;

		return *this;
	}

	std::unique_ptr<Message> next_message()
	{
		DCHECK(socket != nullptr);

		fetch_message();
		if (!has_message()) {
			return nullptr;
		}

		// read header and deadbeef;
		auto header = *reinterpret_cast<Message::header_type *>(buffer + bytes_read);
		auto deadbeef = *reinterpret_cast<Message::deadbeef_type *>(buffer + bytes_read + sizeof(header));

		// check deadbeaf
		DCHECK(deadbeef == Message::DEADBEEF);
		auto message = std::make_unique<Message>();
		auto length = Message::get_message_length(header);
		message->resize(length);

		// copy the data
		DCHECK(bytes_read + length <= bytes_total);
		std::memcpy(message->get_raw_ptr(), buffer + bytes_read, length);
		bytes_read += length;
		DCHECK(bytes_read <= bytes_total);

		return message;
	}

	std::size_t get_read_call_cnt()
	{
		return read_calls;
	}

    private:
	void fetch_message()
	{
		DCHECK(socket != nullptr);

		// return if there is a message left
		if (has_message()) {
			return;
		}

		// copy left bytes
		DCHECK(bytes_read <= bytes_total);
		auto bytes_left = bytes_total - bytes_read;
		bytes_total = 0;

		if (bytes_left > 0 && bytes_read > 0) {
			if (bytes_left <= bytes_read) { // non overlapping
				std::memcpy(buffer, buffer + bytes_read, bytes_left);
			} else {
				for (auto i = 0u; i < bytes_left; i++) {
					buffer[i] = buffer[i + bytes_read];
				}
			}
		}
		bytes_total = bytes_left;
		bytes_read = 0;

		// read new message
                long bytes_received = 0;
                if (use_cxl_transport == false)
		        bytes_received = socket->read_async(buffer + bytes_total, BUFFER_SIZE - bytes_total);
                else
                        bytes_received = cxl_ringbuffer->recv(buffer + bytes_total, BUFFER_SIZE - bytes_total);

		read_calls++;
		if (bytes_received > 0) {
			// successful read
			bytes_total += bytes_received;
		}
	}

	bool has_message()
	{
		// check if the buffer has a message header
		if (bytes_read + Message::get_prefix_size() > bytes_total) {
			return false;
		}

		// read header and deadbeef;
		auto header = *reinterpret_cast<Message::header_type *>(buffer + bytes_read);
		auto deadbeef = *reinterpret_cast<Message::deadbeef_type *>(buffer + bytes_read + sizeof(header));

		// check deadbeaf
		DCHECK(deadbeef == Message::DEADBEEF);

		// check if the buffer has a message
		return bytes_read + Message::get_message_length(header) <= bytes_total;
	}

    public:
	static constexpr uint32_t BUFFER_SIZE = 1024 * 1024 * 4; // 4MB

    private:
        bool use_cxl_transport;
	Socket *socket;
        MPSCRingBuffer *cxl_ringbuffer;
	char buffer[BUFFER_SIZE];
	std::size_t bytes_read, bytes_total;
	std::size_t read_calls = 0;
};
} // namespace star

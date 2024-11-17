//
// Created by Yibo Huang on 8/8/24.
//

#pragma once

#include <atomic>
#include <glog/logging.h>

#include "cxlalloc.h"
#include "common/Message.h"
#include "common/MPSCRingBuffer.h"

namespace star
{

class CXLTransport {
    public:
        CXLTransport(MPSCRingBuffer *cxl_ringbuffers)
                : cxl_ringbuffers(cxl_ringbuffers)
        {}

        void send(Message *message)
        {
                auto dest_node_id = message->get_dest_node_id();
                auto message_length = message->get_message_length();

                uint64_t bytes_sent = 0;
                bytes_sent = cxl_ringbuffers[dest_node_id].send(message->get_raw_ptr(), message_length);
                CHECK(bytes_sent == message_length);
        }

        uint64_t recv(uint64_t src_node_id, char *buffer, uint64_t buffer_size)
        {
                return cxl_ringbuffers[src_node_id].recv(buffer, buffer_size);
        }

    private:
        MPSCRingBuffer *cxl_ringbuffers = nullptr;
};

extern CXLTransport *cxl_transport;

}

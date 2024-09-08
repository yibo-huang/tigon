//
// Created by Yi Lu on 7/22/18.
//

#pragma once

#include "common/LockfreeQueue.h"
#include "common/Message.h"
#include <atomic>
#include <glog/logging.h>
#include <queue>

namespace star
{

class Worker {
    public:
	Worker(std::size_t coordinator_id, std::size_t id)
		: coordinator_id(coordinator_id)
		, id(id)
	{
		n_commit.store(0);
		n_abort_no_retry.store(0);
		n_abort_lock.store(0);
		n_abort_read_validation.store(0);
		n_local.store(0);
		n_si_in_serializable.store(0);
		n_network_size.store(0);
	}

	virtual ~Worker() = default;

	virtual void start() = 0;

	virtual void onExit()
	{
	}

	virtual void start_hstore_master()
	{
	}

	virtual void push_master_special_message(Message *message)
	{
	}

	virtual Message *pop_master_message()
	{
		return nullptr;
	}

	virtual void push_master_message(Message *message)
	{
	}

	virtual void push_message(Message *message) = 0;

	virtual void push_replica_message(Message *message) = 0;

	virtual Message *pop_message() = 0;

    public:
	std::size_t coordinator_id;
	std::size_t id;
	std::atomic<uint64_t> n_commit, n_abort_no_retry, n_abort_lock, n_abort_read_validation, n_local, n_si_in_serializable, n_network_size;

	std::atomic<uint64_t> n_failed_write_lock{ 0 }, n_failed_read_lock{ 0 }, n_failed_no_cmd{ 0 }, n_failed_cmd_not_ready{ 0 };

	std::atomic<uint64_t> last_window_persistence_latency{ 0 };
	std::atomic<uint64_t> last_window_txn_latency{ 0 };
	std::atomic<uint64_t> last_window_queued_lock_req_latency{ 0 };
	std::atomic<uint64_t> last_window_lock_req_latency{ 0 };
	std::atomic<uint64_t> last_window_active_txns{ 0 };

        // Pasha statistics
        std::atomic<uint64_t> n_local_access{ 0 }, n_local_cxl_access{ 0 }, n_remote_access{ 0 }, n_remote_access_with_req{ 0 };
};

} // namespace star

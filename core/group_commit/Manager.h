//
// Created by Yi Lu on 9/10/18.
//

#pragma once

#include "core/Manager.h"

namespace star
{
namespace group_commit
{

class Manager : public star::Manager {
    public:
	using base_type = star::Manager;

	Manager(std::size_t coordinator_id, std::size_t id, const Context &context, std::atomic<bool> &stopFlag)
		: base_type(coordinator_id, id, context, stopFlag)
	{
	}

	void coordinator_start() override
	{
		std::size_t n_workers = context.worker_num;
		std::size_t n_coordinators = context.coordinator_num;

		while (!stopFlag.load()) {
			n_started_workers.store(0);
			n_completed_workers.store(0);
			signal_worker(ExecutorStatus::START);
			wait_all_workers_start();
			std::this_thread::sleep_for(std::chrono::milliseconds(context.group_time));
			set_worker_status(ExecutorStatus::STOP);
			wait_all_workers_finish();
			broadcast_stop();
			wait4_stop(n_coordinators - 1);
			// process replication
			n_completed_workers.store(0);
			set_worker_status(ExecutorStatus::CLEANUP);
			wait_all_workers_finish();
			wait4_ack();
		}

		signal_worker(ExecutorStatus::EXIT);
	}

	void non_coordinator_start() override
	{
		std::size_t n_workers = context.worker_num;
		std::size_t n_coordinators = context.coordinator_num;

		for (;;) {
			ExecutorStatus status = wait4_signal();
			if (status == ExecutorStatus::EXIT) {
				set_worker_status(ExecutorStatus::EXIT);
				break;
			}

			DCHECK(status == ExecutorStatus::START);
			n_completed_workers.store(0);
			n_started_workers.store(0);
			set_worker_status(ExecutorStatus::START);
			wait_all_workers_start();
			wait4_stop(1);
			set_worker_status(ExecutorStatus::STOP);
			wait_all_workers_finish();
			broadcast_stop();
			wait4_stop(n_coordinators - 2);
			// process replication
			n_completed_workers.store(0);
			set_worker_status(ExecutorStatus::CLEANUP);
			wait_all_workers_finish();
			send_ack();
		}
	}
};

} // namespace group_commit
} // namespace star

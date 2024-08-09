//
// Created by Yi Lu on 9/6/18.
//

#pragma once

#include <chrono>

#include "common/Percentile.h"
#include "core/Manager.h"

namespace star
{

class StarManager : public star::Manager {
    public:
	using base_type = star::Manager;

	StarManager(std::size_t coordinator_id, std::size_t id, const Context &context, std::atomic<bool> &stopFlag)
		: base_type(coordinator_id, id, context, stopFlag)
	{
		LOG(INFO) << "batch_size " << context.batch_size;
		batch_size = context.batch_size;
	}

	ExecutorStatus merge_value_to_signal(uint32_t value, ExecutorStatus signal)
	{
		// the value is put into the most significant 24 bits
		uint32_t offset = 8;
		return static_cast<ExecutorStatus>((value << offset) | static_cast<uint32_t>(signal));
	}

	std::tuple<uint32_t, ExecutorStatus> split_signal(ExecutorStatus signal)
	{
		// the value is put into the most significant 24 bits
		uint32_t offset = 8, mask = 0xff;
		uint32_t value = static_cast<uint32_t>(signal);
		// return value and ``real" signal
		return std::make_tuple(value >> offset, static_cast<ExecutorStatus>(value & mask));
	}

	void update_batch_size(uint64_t running_time)
	{
		// running_time in microseconds
		// context.group_time in ms
		batch_size = batch_size * (context.group_time * 1000) / running_time;

		if (batch_size % 10 != 0) {
			batch_size += (10 - batch_size % 10);
		}
	}

	void signal_worker(ExecutorStatus status)
	{
		// only the coordinator node calls this function
		DCHECK(coordinator_id == 0);
		std::tuple<uint32_t, ExecutorStatus> split = split_signal(status);
		set_worker_status(std::get<1>(split));

		// signal to everyone
		for (auto i = 0u; i < context.coordinator_num; i++) {
			if (i == coordinator_id) {
				continue;
			}
			ControlMessageFactory::new_signal_message(*messages[i], static_cast<uint32_t>(status));
		}
		flush_messages();
	}

	void coordinator_start() override
	{
		std::size_t n_workers = context.worker_num;
		std::size_t n_coordinators = context.coordinator_num;

		Percentile<int64_t> all_percentile, c_percentile, s_percentile, batch_size_percentile;

		while (!stopFlag.load()) {
			int64_t ack_wait_time_c = 0, ack_wait_time_s = 0;
			auto c_start = std::chrono::steady_clock::now();
			// start c-phase
			// LOG(INFO) << "start C-Phase";

			n_completed_workers.store(0);
			n_started_workers.store(0);
			batch_size_percentile.add(batch_size);
			signal_worker(merge_value_to_signal(batch_size, ExecutorStatus::C_PHASE));
			wait_all_workers_start();
			wait_all_workers_finish();
			set_worker_status(ExecutorStatus::STOP);
			broadcast_stop();
			wait4_ack();

			{
				auto now = std::chrono::steady_clock::now();
				c_percentile.add(std::chrono::duration_cast<std::chrono::microseconds>(now - c_start).count());
			}

			auto s_start = std::chrono::steady_clock::now();
			// start s-phase

			// LOG(INFO) << "start S-Phase";

			n_completed_workers.store(0);
			n_started_workers.store(0);
			signal_worker(ExecutorStatus::S_PHASE);
			wait_all_workers_start();
			wait_all_workers_finish();
			broadcast_stop();
			wait4_stop(n_coordinators - 1);
			n_completed_workers.store(0);
			set_worker_status(ExecutorStatus::STOP);
			wait_all_workers_finish();
			wait4_ack();
			{
				auto now = std::chrono::steady_clock::now();

				s_percentile.add(std::chrono::duration_cast<std::chrono::microseconds>(now - s_start).count());

				auto all_time = std::chrono::duration_cast<std::chrono::microseconds>(now - c_start).count();

				all_percentile.add(all_time);
				if (context.star_dynamic_batch_size) {
					update_batch_size(all_time);
				}
			}
		}

		signal_worker(ExecutorStatus::EXIT);

		LOG(INFO) << "Average phase switch length " << all_percentile.nth(50) << " us, average c phase length " << c_percentile.nth(50)
			  << " us, average s phase length " << s_percentile.nth(50) << " us, average batch size " << batch_size_percentile.nth(50) << " .";
	}

	void non_coordinator_start() override
	{
		std::size_t n_workers = context.worker_num;
		std::size_t n_coordinators = context.coordinator_num;

		for (;;) {
			ExecutorStatus signal;
			std::tie(batch_size, signal) = split_signal(wait4_signal());

			if (signal == ExecutorStatus::EXIT) {
				set_worker_status(ExecutorStatus::EXIT);
				break;
			}

			// LOG(INFO) << "start C-Phase";

			// start c-phase

			DCHECK(signal == ExecutorStatus::C_PHASE);
			n_completed_workers.store(0);
			n_started_workers.store(0);
			set_worker_status(ExecutorStatus::C_PHASE);
			wait_all_workers_start();
			wait4_stop(1);
			set_worker_status(ExecutorStatus::STOP);
			wait_all_workers_finish();
			send_ack();

			// LOG(INFO) << "start S-Phase";

			// start s-phase

			signal = wait4_signal();
			DCHECK(signal == ExecutorStatus::S_PHASE);
			n_completed_workers.store(0);
			n_started_workers.store(0);
			set_worker_status(ExecutorStatus::S_PHASE);
			wait_all_workers_start();
			wait_all_workers_finish();
			broadcast_stop();
			wait4_stop(n_coordinators - 1);
			set_worker_status(ExecutorStatus::STOP);
			wait_all_workers_finish();
			send_ack();
		}
	}

    public:
	uint32_t batch_size;
};
} // namespace star
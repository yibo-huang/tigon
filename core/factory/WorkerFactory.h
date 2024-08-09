//
// Created by Yi Lu on 9/7/18.
//

#pragma once

#include "core/Defs.h"
#include "core/Executor.h"
#include "core/Manager.h"

#include "benchmark/tpcc/Workload.h"
#include "benchmark/ycsb/Workload.h"

#include "protocol/Silo/Silo.h"
#include "protocol/Silo/SiloExecutor.h"
#include "protocol/TwoPL/TwoPL.h"
#include "protocol/TwoPL/TwoPLExecutor.h"

#include "protocol/H-Store/HStoreExecutor.h"

#include "core/group_commit/Executor.h"
#include "core/group_commit/Manager.h"
#include "protocol/SiloGC/SiloGC.h"
#include "protocol/SiloGC/SiloGCExecutor.h"
#include "protocol/TwoPLGC/TwoPLGC.h"
#include "protocol/TwoPLGC/TwoPLGCExecutor.h"

#include "protocol/Star/Star.h"
#include "protocol/Star/StarExecutor.h"
#include "protocol/Star/StarManager.h"

#include "protocol/Calvin/Calvin.h"
#include "protocol/Calvin/CalvinExecutor.h"
#include "protocol/Calvin/CalvinManager.h"
#include "protocol/Calvin/CalvinTransaction.h"

#include "protocol/Aria/Aria.h"
#include "protocol/Aria/AriaExecutor.h"
#include "protocol/Aria/AriaManager.h"
#include "protocol/Aria/AriaTransaction.h"

#include "protocol/Sundial/Sundial.h"
#include "protocol/Sundial/SundialExecutor.h"

#include <unordered_set>

namespace star
{

template <class Context> class InferType {
};

template <> class InferType<star::tpcc::Context> {
    public:
	template <class Transaction> using WorkloadType = star::tpcc::Workload<Transaction>;
};

template <> class InferType<star::ycsb::Context> {
    public:
	template <class Transaction> using WorkloadType = star::ycsb::Workload<Transaction>;
};

class WorkerFactory {
    public:
	template <class Database, class Context>
	static std::vector<std::shared_ptr<Worker> > create_workers(std::size_t coordinator_id, Database &db, const Context &context,
								    std::atomic<bool> &stop_flag)
	{
		std::unordered_set<std::string> protocols = { "Silo", "SiloGC", "Star", "Sundial", "TwoPL", "TwoPLGC", "Calvin", "HStore", "Aria" };
		CHECK(protocols.count(context.protocol) == 1);

		std::vector<std::shared_ptr<Worker> > workers;

		if (context.protocol == "Silo") {
			using TransactionType = star::SiloTransaction;
			using WorkloadType = typename InferType<Context>::template WorkloadType<TransactionType>;

			auto manager = std::make_shared<Manager>(coordinator_id, context.worker_num, context, stop_flag);

			for (auto i = 0u; i < context.worker_num; i++) {
				workers.push_back(std::make_shared<SiloExecutor<WorkloadType> >(coordinator_id, i, db, context, manager->worker_status,
												manager->n_completed_workers, manager->n_started_workers));
			}

			workers.push_back(manager);

		} else if (context.protocol == "Sundial") {
			using TransactionType = star::SundialTransaction;
			using WorkloadType = typename InferType<Context>::template WorkloadType<TransactionType>;

			auto manager = std::make_shared<Manager>(coordinator_id, context.worker_num, context, stop_flag);

			for (auto i = 0u; i < context.worker_num; i++) {
				workers.push_back(std::make_shared<SundialExecutor<WorkloadType> >(coordinator_id, i, db, context, manager->worker_status,
												   manager->n_completed_workers, manager->n_started_workers));
			}

			workers.push_back(manager);
		} else if (context.protocol == "SiloGC") {
			using TransactionType = star::SiloTransaction;
			using WorkloadType = typename InferType<Context>::template WorkloadType<TransactionType>;

			auto manager = std::make_shared<group_commit::Manager>(coordinator_id, context.worker_num, context, stop_flag);

			for (auto i = 0u; i < context.worker_num; i++) {
				workers.push_back(std::make_shared<SiloGCExecutor<WorkloadType> >(coordinator_id, i, db, context, manager->worker_status,
												  manager->n_completed_workers, manager->n_started_workers));
			}
			workers.push_back(manager);

		} else if (context.protocol == "Star") {
			CHECK(context.partition_num % (context.worker_num * context.coordinator_num) == 0)
				<< "In Star, each partition is managed by only one thread.";

			using TransactionType = star::SiloTransaction;
			using WorkloadType = typename InferType<Context>::template WorkloadType<TransactionType>;

			auto manager = std::make_shared<StarManager>(coordinator_id, context.worker_num, context, stop_flag);

			for (auto i = 0u; i < context.worker_num; i++) {
				workers.push_back(std::make_shared<StarExecutor<WorkloadType> >(coordinator_id, i, db, context, manager->batch_size,
												manager->worker_status, manager->n_completed_workers,
												manager->n_started_workers));
			}
			workers.push_back(manager);

		} else if (context.protocol == "TwoPL") {
			using TransactionType = star::TwoPLTransaction;
			using WorkloadType = typename InferType<Context>::template WorkloadType<TransactionType>;

			auto manager = std::make_shared<Manager>(coordinator_id, context.worker_num, context, stop_flag);

			for (auto i = 0u; i < context.worker_num; i++) {
				workers.push_back(std::make_shared<TwoPLExecutor<WorkloadType> >(coordinator_id, i, db, context, manager->worker_status,
												 manager->n_completed_workers, manager->n_started_workers));
			}

			workers.push_back(manager);
		} else if (context.protocol == "TwoPLGC") {
			using TransactionType = star::TwoPLTransaction;
			using WorkloadType = typename InferType<Context>::template WorkloadType<TransactionType>;

			auto manager = std::make_shared<group_commit::Manager>(coordinator_id, context.worker_num, context, stop_flag);

			for (auto i = 0u; i < context.worker_num; i++) {
				workers.push_back(std::make_shared<TwoPLGCExecutor<WorkloadType> >(coordinator_id, i, db, context, manager->worker_status,
												   manager->n_completed_workers, manager->n_started_workers));
			}

			workers.push_back(manager);
		} else if (context.protocol == "HStore") {
			using TransactionType = star::HStoreTransaction;
			using WorkloadType = typename InferType<Context>::template WorkloadType<TransactionType>;

			auto manager = std::make_shared<Manager>(coordinator_id, context.worker_num, context, stop_flag);

			for (auto i = 0u; i < context.worker_num; i++) {
				workers.push_back(std::make_shared<HStoreExecutor<WorkloadType> >(coordinator_id, i, db, context, manager->worker_status,
												  manager->n_completed_workers, manager->n_started_workers));
			}
			workers.push_back(manager);
		} else if (context.protocol == "Calvin") {
			using TransactionType = star::CalvinTransaction;
			using WorkloadType = typename InferType<Context>::template WorkloadType<TransactionType>;

			// create manager

			auto manager = std::make_shared<CalvinManager<WorkloadType> >(coordinator_id, context.worker_num, db, context, stop_flag);

			// create worker

			std::vector<CalvinExecutor<WorkloadType> *> all_executors;

			for (auto i = 0u; i < context.worker_num; i++) {
				auto w = std::make_shared<CalvinExecutor<WorkloadType> >(coordinator_id, i, db, context, manager->transactions,
											 manager->storages, manager->lock_manager_status,
											 manager->worker_status, manager->n_completed_workers,
											 manager->n_started_workers, manager->active_transactions);
				workers.push_back(w);
				manager->add_worker(w);
				all_executors.push_back(w.get());
			}
			// push manager to workers
			workers.push_back(manager);

			for (auto i = 0u; i < context.worker_num; i++) {
				static_cast<CalvinExecutor<WorkloadType> *>(workers[i].get())->set_all_executors(all_executors);
			}
		} else if (context.protocol == "Aria") {
			using TransactionType = star::AriaTransaction;
			using WorkloadType = typename InferType<Context>::template WorkloadType<TransactionType>;

			// create manager

			auto manager = std::make_shared<AriaManager<WorkloadType> >(coordinator_id, context.worker_num, db, context, stop_flag);

			// create worker

			for (auto i = 0u; i < context.worker_num; i++) {
				workers.push_back(std::make_shared<AriaExecutor<WorkloadType> >(
					coordinator_id, i, db, context, manager->transactions, manager->storages, manager->epoch, manager->worker_status,
					manager->total_abort, manager->n_completed_workers, manager->n_started_workers));
			}

			workers.push_back(manager);
		}

		return workers;
	}
};
} // namespace star
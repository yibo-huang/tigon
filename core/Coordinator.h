//
// Created by Yi Lu on 7/24/18.
//

#pragma once

#include "common/LockfreeQueue.h"
#include "common/Message.h"
#include "common/Socket.h"
#include "common/CXLMemory.h"
#include "common/MPSCRingBuffer.h"
#include "common/CXLTransport.h"
#include "common/CXL_EBR.h"
#include "core/ControlMessage.h"
#include "core/Dispatcher.h"
#include "core/Executor.h"
#include "core/Worker.h"
#include "core/factory/WorkerFactory.h"
#include <boost/algorithm/string.hpp>
#include <glog/logging.h>
#include <thread>
#include <vector>
#include <chrono>
#include <memory>

#include "protocol/Pasha/MigrationManager.h"

namespace star
{
bool warmed_up = false;
class Coordinator {
    public:
	template <class Database, class Context>
	Coordinator(std::size_t id, Database &db, Context &context)
		: id(id)
		, coordinator_num(context.peers.size())
		, peers(context.peers)
		, context(context)
	{
                // init flags
                workerStopFlag.store(false);
		ioStopFlag.store(false);

                // init cxlalloc
                cxl_memory.init(context);
                cxl_memory.init_cxlalloc_for_given_thread(context.worker_num + 1, 0, context.coordinator_num, context.coordinator_id);

                // init CXL transport
                initCXLTransport();

                // init CXL EBR
                initCXLEBR();

                // init logger
                if (context.log_path != "" && context.wal_group_commit_time != 0) {
                        std::string redo_filename = context.log_path + "_group_commit.txt";
                        std::string logger_type;
                        if (context.lotus_checkpoint == LotusCheckpointScheme::COW_ON_CHECKPOINT_ON_LOGGING_OFF) { // logging off so that logging and checkpoint threads
                                                                                                                // will not compete for bandwidth
                                logger_type = "Blackhole Logger";
                                context.master_logger = new star::BlackholeLogger(redo_filename, context.emulated_persist_latency);
                        } else {
                                logger_type = "GroupCommit Logger";

                                // init CXL global epoch (std::atomic<uint64_t>)
                                std::atomic<uint64_t> *cxl_global_epoch = nullptr;
                                if (context.coordinator_id == 0) {
                                        cxl_global_epoch = reinterpret_cast<std::atomic<uint64_t> *>(cxl_memory.cxlalloc_malloc_wrapper(sizeof(std::atomic<uint64_t>), CXLMemory::MISC_ALLOCATION));
                                        CXLMemory::commit_shared_data_initialization(CXLMemory::cxl_global_epoch_root_index, cxl_global_epoch);
                                } else {
                                        void *tmp = NULL;
                                        CXLMemory::wait_and_retrieve_cxl_shared_data(CXLMemory::cxl_global_epoch_root_index, &tmp);
                                        cxl_global_epoch = reinterpret_cast<std::atomic<uint64_t> *>(tmp);
                                }

                                std::vector<star::LockfreeLogBufferQueue *> *log_buffer_queues = new std::vector<star::LockfreeLogBufferQueue *>();
                                CHECK(log_buffer_queues != nullptr);
                                for (auto i = 0; i < context.worker_num; i++) {
                                        star::LockfreeLogBufferQueue *log_buffer_queue = new star::LockfreeLogBufferQueue();
                                        log_buffer_queues->push_back(log_buffer_queue);
                                        context.slave_loggers.push_back(new star::PashaGroupCommitLoggerSlave(log_buffer_queue, cxl_global_epoch));
                                }
                                context.master_logger = new star::PashaGroupCommitLogger(redo_filename, log_buffer_queues, cxl_global_epoch, ioStopFlag,
                                                context.group_commit_batch_size, context.wal_group_commit_time, context.emulated_persist_latency);
                        }
                        LOG(INFO) << "WAL Group Commiting to file [" << redo_filename << "]" << " using " << logger_type;
                } else {
                        std::string redo_filename = context.log_path + "_non_group_commit.txt";
                        std::string logger_type;
                        if (context.lotus_checkpoint == LotusCheckpointScheme::COW_OFF_CHECKPOINT_OFF_LOGGING_OFF) {
                                logger_type = "Blackhole Logger";
                                context.master_logger = new star::BlackholeLogger(redo_filename, context.emulated_persist_latency);
                        } else {
                                logger_type = "SimpleWAL Logger";
                                context.master_logger = new star::SimpleWALLogger(redo_filename, context.emulated_persist_latency);
                        }
                        LOG(INFO) << "WAL Group Commiting off. Log to file " << redo_filename << " using " << logger_type;
                }

                // init workers
		LOG(INFO) << "Coordinator initializes " << context.worker_num << " workers.";
		workers = WorkerFactory::create_workers(id, db, context, workerStopFlag);

		// init sockets vector
		inSockets.resize(context.io_thread_num);
		outSockets.resize(context.io_thread_num);

		for (auto i = 0u; i < context.io_thread_num; i++) {
			inSockets[i].resize(peers.size());
			outSockets[i].resize(peers.size());
		}
	}

	~Coordinator() = default;

	void sendMessage(Message *message, Socket &dest_socket)
	{
		auto dest_node_id = message->get_dest_node_id();
		DCHECK(message->get_message_length() == message->data.length());

		dest_socket.write_n_bytes(message->get_raw_ptr(), message->get_message_length());
	}

	void measure_round_trip()
	{
		auto init_message = [](Message *message, std::size_t coordinator_id, std::size_t dest_node_id) {
			message->set_source_node_id(coordinator_id);
			message->set_dest_node_id(dest_node_id);
			message->set_worker_id(0);
		};
		Percentile<uint64_t> round_trip_latency;
		if (id == 0) {
			int i = 0;
			BufferedReader reader(inSockets[0][1]);
			while (i < 1000) {
				++i;
				auto r_start = std::chrono::steady_clock::now();
				// LOG(INFO) << "Message " << i << " to";
				auto message = std::make_unique<Message>();
				init_message(message.get(), 0, 1);
				ControlMessageFactory::new_statistics_message(*message, id, 0, 0, 0, 0, 0, 0, 0);
				sendMessage(message.get(), outSockets[0][1]);
				while (true) {
					auto message = reader.next_message();
					if (message == nullptr) {
						std::this_thread::yield();
						continue;
					}
					break;
				}
				auto ltc = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - r_start).count();
				round_trip_latency.add(ltc);
				// LOG(INFO) << "Message " << i << " back";
			}
			LOG(INFO) << "round_trip_latency " << round_trip_latency.nth(50) << " (50th) " << round_trip_latency.nth(75) << " (75th) "
				  << round_trip_latency.nth(95) << " (95th) " << round_trip_latency.nth(95) << " (99th) ";
		} else if (id == 1) {
			BufferedReader reader(inSockets[0][0]);
			int i = 0;
			while (i < 1000) {
				while (true) {
					auto message = reader.next_message();
					if (message == nullptr) {
						std::this_thread::yield();
						continue;
					}
					break;
				}
				auto message = std::make_unique<Message>();
				init_message(message.get(), 1, 0);
				ControlMessageFactory::new_statistics_message(*message, id, 0, 0, 0, 0, 0, 0, 0);
				sendMessage(message.get(), outSockets[0][0]);
				++i;
			}
		}
		// exit(0);
	}
	void start()
	{
		// init dispatcher vector
		iDispatchers.resize(context.io_thread_num);
		oDispatchers.resize(context.io_thread_num);

		// measure_round_trip();

		// start dispatcher threads
		std::vector<std::thread> iDispatcherThreads, oDispatcherThreads;

		for (auto i = 0u; i < context.io_thread_num; i++) {
			iDispatchers[i] = std::make_unique<IncomingDispatcher>(id, i, context.io_thread_num, inSockets[i], cxl_ringbuffers, workers, in_queue, 
                                                                               out_to_in_queue, ioStopFlag, context);
			oDispatchers[i] = std::make_unique<OutgoingDispatcher>(id, i, context.io_thread_num, outSockets[i], workers, out_queue,
                                                                               out_to_in_queue, ioStopFlag, context);

                        // the input thread is always needed
			iDispatcherThreads.emplace_back(&IncomingDispatcher::start, iDispatchers[i].get());
                        pin_thread_to_core(iDispatcherThreads[i]);

                        // but the output thread is optional
                        if (context.use_output_thread == true) {
			        oDispatcherThreads.emplace_back(&OutgoingDispatcher::start, oDispatchers[i].get());
                                pin_thread_to_core(oDispatcherThreads[i]);
                        } else {
                                CHECK(context.use_cxl_transport == true);
                        }
		}

                std::vector<std::thread> logger_threads;
                if (context.log_path != "" && context.wal_group_commit_time != 0 && context.lotus_checkpoint != LotusCheckpointScheme::COW_ON_CHECKPOINT_ON_LOGGING_OFF) {
                        logger_threads.emplace_back(&PashaGroupCommitLogger::start, reinterpret_cast<PashaGroupCommitLogger *>(context.master_logger));
                        pin_thread_to_core(logger_threads[0]);
                }

		std::vector<std::thread> threads;

		LOG(INFO) << "Coordinator starts to run " << workers.size() << " workers.";

		for (auto i = 0u; i < workers.size(); i++) {
			threads.emplace_back(&Worker::start, workers[i].get());

			if (i != workers.size() - 1) {
                                pin_thread_to_core(threads[i]);
                        }
		}

		// run timeToRun seconds
		auto timeToRun = context.time_to_run, warmup = context.time_to_warmup, cooldown = 0;
		auto startTime = std::chrono::steady_clock::now();

		uint64_t total_commit = 0, total_abort_no_retry = 0, total_abort_lock = 0, total_abort_read_validation = 0, total_local = 0,
			 total_si_in_serializable = 0, total_network_size = 0,
                         total_local_access = 0, total_local_cxl_access = 0, total_remote_access = 0, total_remote_access_with_req = 0,
                         total_data_move_in = 0, total_data_move_out = 0;
		int count = 0;

		do {
			std::this_thread::sleep_for(std::chrono::seconds(1));

			uint64_t n_commit = 0, n_abort_no_retry = 0, n_abort_lock = 0, n_abort_read_validation = 0, n_local = 0, n_si_in_serializable = 0,
				 n_network_size = 0;
			uint64_t total_persistence_latency = 0;
			uint64_t total_txn_latency = 0;
			uint64_t total_queued_lock_latency = 0;
			uint64_t total_active_txns = 0;
			uint64_t total_lock_latency = 0;
			uint64_t n_failed_read_lock = 0, n_failed_write_lock = 0, n_failed_no_cmd = 0, n_failed_cmd_not_ready = 0;
                        uint64_t n_local_access = 0, n_local_cxl_access = 0, n_remote_access = 0, n_remote_access_with_req = 0;
                        uint64_t n_data_move_in = 0, n_data_move_out = 0;
			for (auto i = 0u; i < workers.size(); i++) {
				n_failed_read_lock += workers[i]->n_failed_read_lock;
				workers[i]->n_failed_read_lock.store(0);

				n_failed_write_lock += workers[i]->n_failed_write_lock;
				workers[i]->n_failed_write_lock.store(0);

				n_failed_no_cmd += workers[i]->n_failed_no_cmd;
				workers[i]->n_failed_no_cmd.store(0);

				n_failed_cmd_not_ready += workers[i]->n_failed_cmd_not_ready;
				workers[i]->n_failed_cmd_not_ready.store(0);

				n_commit += workers[i]->n_commit.load();
				workers[i]->n_commit.store(0);

				n_abort_no_retry += workers[i]->n_abort_no_retry.load();
				workers[i]->n_abort_no_retry.store(0);

				n_abort_lock += workers[i]->n_abort_lock.load();
				workers[i]->n_abort_lock.store(0);

				n_abort_read_validation += workers[i]->n_abort_read_validation.load();
				workers[i]->n_abort_read_validation.store(0);

				n_local += workers[i]->n_local.load();
				workers[i]->n_local.store(0);

				n_si_in_serializable += workers[i]->n_si_in_serializable.load();
				workers[i]->n_si_in_serializable.store(0);

				n_network_size += workers[i]->n_network_size.load();
				workers[i]->n_network_size.store(0);

				total_persistence_latency += workers[i]->last_window_persistence_latency.load();
				total_txn_latency += workers[i]->last_window_txn_latency.load();
				total_queued_lock_latency += workers[i]->last_window_queued_lock_req_latency.load();
				total_lock_latency += workers[i]->last_window_lock_req_latency.load();
				total_active_txns += workers[i]->last_window_active_txns.load();

                                n_local_access += workers[i]->n_local_access.load();
				workers[i]->n_local_access.store(0);

                                n_local_cxl_access += workers[i]->n_local_cxl_access.load();
				workers[i]->n_local_cxl_access.store(0);

                                n_remote_access += workers[i]->n_remote_access.load();
				workers[i]->n_remote_access.store(0);

                                n_remote_access_with_req += workers[i]->n_remote_access_with_req.load();
				workers[i]->n_remote_access_with_req.store(0);
			}

                        n_data_move_out += num_data_move_out;
                        n_data_move_in += num_data_move_in;
                        num_data_move_out.store(0);
                        num_data_move_in.store(0);

			LOG(INFO) << "commit: " << n_commit << " abort: " << n_abort_no_retry + n_abort_lock + n_abort_read_validation << " ("
				  << n_abort_no_retry << "/" << n_abort_lock << "/" << n_abort_read_validation << "), persistence latency "
				  << total_persistence_latency / (workers.size() - 1) << ", txn latency " << total_txn_latency / (workers.size() - 1)
				  << ", queued lock latency " << total_queued_lock_latency / (workers.size() - 1) << ", lock latency "
				  << total_lock_latency / (workers.size() - 1) << ", active_txns " << total_active_txns / (workers.size() - 1)
				  << ", n_failed_read_lock " << n_failed_read_lock << ", n_failed_write_lock " << n_failed_write_lock
				  << ", n_failed_cmd_not_ready " << n_failed_cmd_not_ready << ", n_failed_no_cmd " << n_failed_no_cmd
				  << ", network size: " << n_network_size << ", avg network size: " << 1.0 * n_network_size / n_commit
				  << ", si_in_serializable: " << n_si_in_serializable << " " << 100.0 * n_si_in_serializable / n_commit << " %"
				  << ", local: " << 100.0 * n_local / n_commit << " %"
                                  << ", local_access: " << n_local_access
                                  << ", local_cxl_access: " << n_local_cxl_access << " (" << 100.0 * n_local_cxl_access / n_local_access << "%)"
                                  << ", remote_access: " << n_remote_access
                                  << ", remote_access_with_req: " << n_remote_access_with_req << " (" << 100.0 * n_remote_access_with_req / n_remote_access << "%)"
                                  << ", data_move_in: " << n_data_move_in
                                  << ", data_move_out: " << n_data_move_out;
			count++;
			if (count > warmup && count <= timeToRun - cooldown) {
				warmed_up = true;
				total_commit += n_commit;
				total_abort_no_retry += n_abort_no_retry;
				total_abort_lock += n_abort_lock;
				total_abort_read_validation += n_abort_read_validation;
				total_local += n_local;
				total_si_in_serializable += n_si_in_serializable;
				total_network_size += n_network_size;
                                total_local_access += n_local_access;
                                total_local_cxl_access += n_local_cxl_access;
                                total_remote_access += n_remote_access;
                                total_remote_access_with_req += n_remote_access_with_req;
                                total_data_move_in += n_data_move_in;
                                total_data_move_out += n_data_move_out;
			}

		} while (std::chrono::duration_cast<std::chrono::seconds>(std::chrono::steady_clock::now() - startTime).count() < timeToRun);

		count = timeToRun - warmup - cooldown;
		double abort_rate = (total_abort_lock) / (total_commit + total_abort_lock + 0.0);
		LOG(INFO) << "average commit: " << 1.0 * total_commit / count
			  << " abort: " << 1.0 * (total_abort_no_retry + total_abort_lock + total_abort_read_validation) / count << " ("
			  << 1.0 * total_abort_no_retry / count << "/" << 1.0 * total_abort_lock / count << "/" << 1.0 * total_abort_read_validation / count
			  << "), abort_rate: " << abort_rate << ", network size: " << total_network_size
			  << ", avg network size: " << 1.0 * total_network_size / total_commit << ", si_in_serializable: " << total_si_in_serializable << " "
			  << 100.0 * total_si_in_serializable / total_commit << " %"
			  << ", local: " << 100.0 * total_local / total_commit << " %"
                          << ", local_access: " << total_local_access
                          << ", local_cxl_access: " << total_local_cxl_access << " (" << 100.0 * total_local_cxl_access / total_local_access << "%)"
                          << ", remote_access: " << total_remote_access
                          << ", remote_access_with_req: " << total_remote_access_with_req << " (" << 100.0 * total_remote_access_with_req / total_remote_access << "%)"
                          << ", data_move_in: " << total_data_move_in
                          << ", data_move_out: " << total_data_move_out;

		workerStopFlag.store(true);

		for (auto i = 0u; i < threads.size(); i++) {
			workers[i]->onExit();
			threads[i].join();
		}

                // print CXL memory usage
                cxl_memory.print_stats();

                // print software cache-coherence stats
                if (scc_manager != nullptr)
                        scc_manager->print_stats();

		// gather throughput
		gather_and_print(1.0 * total_commit / count,
                                cxl_memory.get_stats(CXLMemory::INDEX_USAGE),
                                cxl_memory.get_stats(CXLMemory::METADATA_USAGE),
                                cxl_memory.get_stats(CXLMemory::DATA_USAGE),
                                cxl_memory.get_stats(CXLMemory::TRANSPORT_USAGE),
                                cxl_memory.get_stats(CXLMemory::MISC_USAGE),
                                cxl_memory.get_stats(CXLMemory::TOTAL_HW_CC_USAGE));

		// make sure all messages are sent
		std::this_thread::sleep_for(std::chrono::seconds(1));

		ioStopFlag.store(true);

		for (auto i = 0u; i < context.io_thread_num; i++) {
			iDispatcherThreads[i].join();
                        if (context.use_output_thread == true) {
			        oDispatcherThreads[i].join();
                        } else {
                                CHECK(context.use_cxl_transport == true);
                        }
		}

                if (context.log_path != "" && context.wal_group_commit_time != 0 && context.lotus_checkpoint != LotusCheckpointScheme::COW_ON_CHECKPOINT_ON_LOGGING_OFF) {
                        logger_threads[0].join();
                }

		if (context.master_logger != nullptr) {
			context.master_logger->print_sync_stats();
                }

		measure_round_trip();
		close_sockets();

		LOG(INFO) << "Coordinator exits.";
	}

        void initCXLTransport()
        {
                int i = 0;
                void *tmp = NULL;

                if (id == 0) {
                        cxl_ringbuffers = reinterpret_cast<MPSCRingBuffer *>(cxl_memory.cxlalloc_malloc_wrapper(sizeof(MPSCRingBuffer) * coordinator_num, 
                                CXLMemory::TRANSPORT_ALLOCATION));
                        for (i = 0; i < coordinator_num; i++)
                                new(&cxl_ringbuffers[i]) MPSCRingBuffer(context.cxl_trans_entry_struct_size, context.cxl_trans_entry_num);
                        cxl_transport = new CXLTransport(cxl_ringbuffers);
                        CXLMemory::commit_shared_data_initialization(CXLMemory::cxl_transport_root_index, cxl_ringbuffers);
                        LOG(INFO) << "Coordinator " << id << " initializes CXL transport metadata ("
                                << coordinator_num << " ringbuffers each with " << cxl_ringbuffers[0].get_entry_num() << " entries (each "
                                << cxl_ringbuffers[0].get_entry_size() << " Bytes)";
                } else {
                        CXLMemory::wait_and_retrieve_cxl_shared_data(CXLMemory::cxl_transport_root_index, &tmp);
                        cxl_ringbuffers = reinterpret_cast<MPSCRingBuffer *>(tmp);
                        cxl_transport = new CXLTransport(cxl_ringbuffers);
                        LOG(INFO) << "Coordinator " << id << " retrives CXL transport metadata ("
                                << coordinator_num << " ringbuffers each with " << cxl_ringbuffers[0].get_entry_num() << " entries (each "
                                << cxl_ringbuffers[0].get_entry_size() << " Bytes)";
                }
        }

        void initCXLEBR()
        {
                int i = 0;
                void *tmp = NULL;

                if (id == 0) {
                        global_ebr_meta = reinterpret_cast<CXL_EBR *>(cxl_memory.cxlalloc_malloc_wrapper(sizeof(CXL_EBR), CXLMemory::MISC_ALLOCATION));
                        new(global_ebr_meta) CXL_EBR(context.coordinator_num, context.worker_num);
                        CXLMemory::commit_shared_data_initialization(CXLMemory::cxl_global_ebr_meta_root_index, global_ebr_meta);
                        LOG(INFO) << "created global CXL EBR metadata";
                } else {
                        CXLMemory::wait_and_retrieve_cxl_shared_data(CXLMemory::cxl_global_ebr_meta_root_index, &tmp);
                        global_ebr_meta = reinterpret_cast<CXL_EBR *>(tmp);
                        LOG(INFO) << "retrieved global CXL EBR metadata";
                }
        }

	void connectToPeers()
	{
		// single node test mode
		if (peers.size() == 1) {
			return;
		}

		auto getAddressPort = [](const std::string &addressPort) {
			std::vector<std::string> result;
			boost::algorithm::split(result, addressPort, boost::is_any_of(":"));
			return result;
		};

		// start some listener threads

		std::vector<std::thread> listenerThreads;

		for (auto i = 0u; i < context.io_thread_num; i++) {
			listenerThreads.emplace_back(
				[id = this->id, peers = this->peers, &inSockets = this->inSockets[i], &getAddressPort, tcp_quick_ack = context.tcp_quick_ack,
				 tcp_no_delay = context.tcp_no_delay](std::size_t listener_id) {
					std::vector<std::string> addressPort = getAddressPort(peers[id]);

					Listener l(addressPort[0].c_str(), atoi(addressPort[1].c_str()) + listener_id, 100);
					LOG(INFO) << "Listener " << listener_id << " on coordinator " << id << " listening on " << peers[id] << " tcp_no_delay "
						  << tcp_no_delay << " tcp_quick_ack " << tcp_quick_ack;

					auto n = peers.size();

					for (std::size_t i = 0; i < n - 1; i++) {
						Socket socket = l.accept();
						std::size_t c_id;
						socket.read_number(c_id);
						// set quick ack flag
						if (tcp_no_delay) {
							socket.disable_nagle_algorithm();
						}
						socket.set_quick_ack_flag(tcp_quick_ack);
						inSockets[c_id] = std::move(socket);
						LOG(INFO) << "Listener accepted connection from coordinator " << c_id;
					}

					LOG(INFO) << "Listener " << listener_id << " on coordinator " << id << " exits.";
				},
				i);
		}

		// connect to peers
		auto n = peers.size();
		constexpr std::size_t retryLimit = 50;

		// connect to multiple remote coordinators
		for (auto i = 0u; i < n; i++) {
			if (i == id)
				continue;
			std::vector<std::string> addressPort = getAddressPort(peers[i]);
			// connnect to multiple remote listeners
			for (auto listener_id = 0u; listener_id < context.io_thread_num; listener_id++) {
				for (auto k = 0u; k < retryLimit; k++) {
					Socket socket;

					int ret = socket.connect(addressPort[0].c_str(), atoi(addressPort[1].c_str()) + listener_id);
					if (ret == -1) {
						socket.close();
						if (k == retryLimit - 1) {
							LOG(FATAL) << "failed to connect to peers, exiting ...";
							exit(1);
						}

						// listener on the other side has not been set up.
						LOG(INFO) << "Coordinator " << id << " failed to connect " << i << "(" << peers[i] << ")'s listener "
							  << listener_id << ", retry in 5 seconds.";
						std::this_thread::sleep_for(std::chrono::seconds(5));
						continue;
					}
					if (context.tcp_no_delay) {
						socket.disable_nagle_algorithm();
					}

					LOG(INFO) << "Coordinator " << id << " connected to " << i;
					socket.write_number(id);
					outSockets[listener_id][i] = std::move(socket);
					break;
				}
			}
		}

		for (auto i = 0u; i < listenerThreads.size(); i++) {
			listenerThreads[i].join();
		}

		LOG(INFO) << "Coordinator " << id << " connected to all peers.";
	}

	void gather_and_print(double commit, uint64_t size_index_usage, uint64_t size_metadata_usage, uint64_t size_data_usage, uint64_t size_transport_usage, uint64_t size_misc_usage, uint64_t size_hwcc_usage)
	{
		auto init_message = [](Message *message, std::size_t coordinator_id, std::size_t dest_node_id) {
			message->set_source_node_id(coordinator_id);
			message->set_dest_node_id(dest_node_id);
			message->set_worker_id(0);
		};

		double replica_sum = 0;

		if (id == 0) {
			auto partitioner = PartitionerFactory::create_partitioner(context.partitioner, id, context.coordinator_num);
			for (std::size_t i = 0; i < coordinator_num - 1; i++) {
				in_queue.wait_till_non_empty();
				std::unique_ptr<Message> message(in_queue.front());
				bool ok = in_queue.pop();
				CHECK(ok);
				CHECK(message->get_message_count() == 1);

				MessagePiece messagePiece = *(message->begin());

				CHECK(messagePiece.get_message_type() == static_cast<uint32_t>(ControlMessage::STATISTICS));
				CHECK(messagePiece.get_message_length() == MessagePiece::get_header_size() + sizeof(int) + sizeof(double) + 6 * sizeof(uint64_t));
				Decoder dec(messagePiece.toStringPiece());
				int coordinator_id;
				double r_commit;
                                uint64_t r_size_index_usage, r_size_metadata_usage, r_size_data_usage, r_size_transport_usage, r_size_misc_usage, r_size_hwcc_usage;
				dec >> coordinator_id >> r_commit >> r_size_index_usage >> r_size_metadata_usage >> r_size_data_usage >> r_size_transport_usage >> r_size_misc_usage >> r_size_hwcc_usage;
				if (context.partitioner == "hpb") {
					if (coordinator_id < (int)partitioner->num_coordinator_for_one_replica()) {
						commit += r_commit;
					} else {
						replica_sum += r_commit;
					}
				} else {
					commit += r_commit;
				}
                                size_index_usage += r_size_index_usage;
                                size_metadata_usage += r_size_metadata_usage;
                                size_data_usage += r_size_data_usage;
                                size_transport_usage += r_size_transport_usage;
                                size_misc_usage += r_size_misc_usage;
                                size_hwcc_usage += r_size_hwcc_usage;
			}

		} else {
			auto message = std::make_unique<Message>();
			init_message(message.get(), id, 0);
			ControlMessageFactory::new_statistics_message(*message, id, commit, size_index_usage, size_metadata_usage, size_data_usage, size_transport_usage, size_misc_usage, size_hwcc_usage);
                        if (context.use_output_thread == true) {
			        out_queue.push(message.release());
                                // message is reclaimed by the output thread
                        } else {
                                cxl_transport->send(message.get());
                                // must reclaim the message here - otherwise memory leakage would occur
                                // we do it by not releasing it
                        }
		}
		if (context.partitioner == "hpb") {
			LOG(INFO) << "replica total commit " << replica_sum;
		}

                // print stats
                if (id == 0) {
                        LOG(INFO) << "Global Stats:"
                                  << " total_commit: " << commit
                                  << " total_size_index_usage: " << size_index_usage
                                  << " total_size_metadata_usage: " << size_metadata_usage
                                  << " total_size_data_usage: " << size_data_usage
                                  << " total_size_transport_usage: " << size_transport_usage
                                  << " total_size_misc_usage: " << size_misc_usage
                                  << " total_hw_cc_usage: " << size_hwcc_usage
                                  << " total_usage: " << size_index_usage + size_metadata_usage + size_data_usage + size_transport_usage + size_misc_usage;
                }
	}

    private:
	void close_sockets()
	{
		for (auto i = 0u; i < inSockets.size(); i++) {
			for (auto j = 0u; j < inSockets[i].size(); j++) {
				inSockets[i][j].close();
			}
		}
		for (auto i = 0u; i < outSockets.size(); i++) {
			for (auto j = 0u; j < outSockets[i].size(); j++) {
				outSockets[i][j].close();
			}
		}
	}

	void pin_thread_to_core(std::thread &t)
	{
		static std::atomic<uint64_t> core_id{ 0 };
		LOG(INFO) << "pinned thread to core " << core_id;
		cpu_set_t cpuset;
		CPU_ZERO(&cpuset);
		CPU_SET(core_id++, &cpuset);
		int rc = pthread_setaffinity_np(t.native_handle(), sizeof(cpu_set_t), &cpuset);
		CHECK(rc == 0);
	}

    private:
	/*
	 * A coordinator may have multilpe inSockets and outSockets, connected to one
	 * remote coordinator to fully utilize the network
	 *
	 * inSockets[0][i] receives w_id % io_threads from coordinator i
	 */

	std::size_t id, coordinator_num;
	const std::vector<std::string> &peers;
	Context &context;
	std::vector<std::vector<Socket> > inSockets, outSockets;
	std::atomic<bool> workerStopFlag, ioStopFlag;
	std::vector<std::shared_ptr<Worker> > workers;
	std::vector<std::unique_ptr<IncomingDispatcher> > iDispatchers;
	std::vector<std::unique_ptr<OutgoingDispatcher> > oDispatchers;
	LockfreeQueue<Message *> in_queue, out_queue;
	// Side channel that connects oDispatcher to iDispatcher.
	// Useful for transfering messages between partitions for HStore.
	LockfreeQueue<Message *> out_to_in_queue;

        MPSCRingBuffer *cxl_ringbuffers;
};
} // namespace star

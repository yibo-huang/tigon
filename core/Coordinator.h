//
// Created by Yi Lu on 7/24/18.
//

#pragma once

#include "common/LockfreeQueue.h"
#include "common/Message.h"
#include "common/Socket.h"
#include "common/CXLMemory.h"
#include "common/MPSCRingBuffer.h"
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
	Coordinator(std::size_t id, Database &db, const Context &context)
		: id(id)
		, coordinator_num(context.peers.size())
		, peers(context.peers)
		, context(context)
	{
                CXLMemory::init_cxlalloc_for_given_thread(context.worker_num + 1, 0, context.coordinator_num, context.coordinator_id);

		workerStopFlag.store(false);
		ioStopFlag.store(false);
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
				ControlMessageFactory::new_statistics_message(*message, id, 0);
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
				ControlMessageFactory::new_statistics_message(*message, id, 0);
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
			oDispatchers[i] = std::make_unique<OutgoingDispatcher>(id, i, context.io_thread_num, outSockets[i], cxl_ringbuffers, workers, out_queue, 
                                                                               out_to_in_queue, ioStopFlag, context);

			iDispatcherThreads.emplace_back(&IncomingDispatcher::start, iDispatchers[i].get());
			oDispatcherThreads.emplace_back(&OutgoingDispatcher::start, oDispatchers[i].get());
			if (context.cpu_affinity) {
				pin_thread_to_core(iDispatcherThreads[i]);
				pin_thread_to_core(oDispatcherThreads[i]);
			}
		}

		std::vector<std::thread> threads;

		LOG(INFO) << "Coordinator starts to run " << workers.size() << " workers.";

		for (auto i = 0u; i < workers.size(); i++) {
			threads.emplace_back(&Worker::start, workers[i].get());

			if (context.cpu_affinity) {
				pin_thread_to_core(threads[i]);
			}
		}

		// run timeToRun seconds
		auto timeToRun = 30, warmup = 5, cooldown = 0;
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

                                n_data_move_in += num_data_move_in;
                                num_data_move_in.store(0);
                                n_data_move_out += num_data_move_out;
                                num_data_move_out.store(0);
			}

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
                                  << ", n_data_move_out: " << n_data_move_out;
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

		// gather throughput
		double sum_commit = gather(1.0 * total_commit / count);
		if (id == 0) {
			LOG(INFO) << "total commit: " << sum_commit;
		}

		// make sure all messages are sent
		std::this_thread::sleep_for(std::chrono::seconds(1));

		ioStopFlag.store(true);

		for (auto i = 0u; i < context.io_thread_num; i++) {
			iDispatcherThreads[i].join();
			oDispatcherThreads[i].join();
		}

		if (context.logger)
			context.logger->print_sync_stats();
		measure_round_trip();
		close_sockets();

		LOG(INFO) << "Coordinator exits.";
	}

        void initCXLTransport()
        {
                int i = 0;
                void *tmp = NULL;

                if (id == 0) {
                        cxl_ringbuffers = reinterpret_cast<MPSCRingBuffer *>(CXLMemory::cxlalloc_malloc_wrapper(sizeof(MPSCRingBuffer) * coordinator_num));
                        for (i = 0; i < coordinator_num; i++)
                                new(&cxl_ringbuffers[i]) MPSCRingBuffer(context.cxl_trans_entry_num);
                        CXLMemory::commit_shared_data_initialization(CXLMemory::cxl_transport_root_index, cxl_ringbuffers);
                        LOG(INFO) << "Coordinator " << id << " initializes CXL transport metadata ("
                                << coordinator_num << " ringbuffers each with " << cxl_ringbuffers[0].get_entry_num() << " entries (each "
                                << cxl_ringbuffers[0].get_entry_size() << " Bytes)";
                } else {
                        CXLMemory::wait_and_retrieve_cxl_shared_data(CXLMemory::cxl_transport_root_index, &tmp);
                        cxl_ringbuffers = reinterpret_cast<MPSCRingBuffer *>(tmp);
                        LOG(INFO) << "Coordinator " << id << " retrives CXL transport metadata ("
                                << coordinator_num << " ringbuffers each with " << cxl_ringbuffers[0].get_entry_num() << " entries (each "
                                << cxl_ringbuffers[0].get_entry_size() << " Bytes)";
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

	double gather(double value)
	{
		auto init_message = [](Message *message, std::size_t coordinator_id, std::size_t dest_node_id) {
			message->set_source_node_id(coordinator_id);
			message->set_dest_node_id(dest_node_id);
			message->set_worker_id(0);
		};

		double sum = value;
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
				CHECK(messagePiece.get_message_length() == MessagePiece::get_header_size() + sizeof(double) + sizeof(int));
				Decoder dec(messagePiece.toStringPiece());
				int coordinator_id;
				double v;
				dec >> coordinator_id >> v;
				if (context.partitioner == "hpb") {
					if (coordinator_id < (int)partitioner->num_coordinator_for_one_replica()) {
						sum += v;
					} else {
						replica_sum += v;
					}
				} else {
					sum += v;
				}
			}

		} else {
			auto message = std::make_unique<Message>();
			init_message(message.get(), id, 0);
			ControlMessageFactory::new_statistics_message(*message, id, value);
			out_queue.push(message.release());
		}
		if (context.partitioner == "hpb") {
			LOG(INFO) << "replica total commit " << replica_sum;
		}
		return sum;
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
#ifndef __APPLE__
		static std::size_t core_id = context.cpu_core_id;
		LOG(INFO) << "pinned thread to core " << core_id;
		cpu_set_t cpuset;
		CPU_ZERO(&cpuset);
		CPU_SET(core_id++, &cpuset);
		int rc = pthread_setaffinity_np(t.native_handle(), sizeof(cpu_set_t), &cpuset);
		CHECK(rc == 0);
#endif
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
	Context context;
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

//
// Created by Yi Lu on 3/21/19.
//

#pragma once

#include <glog/logging.h>
#include <chrono>
#include <thread>
#include <cstring>
#include <string>
#include <fcntl.h>
#include <stdio.h>
#include <mutex>
#include <atomic>

#include "common/Percentile.h"
#include "common/LockfreeQueue.h"

#include "BufferedFileWriter.h"
#include "Time.h"

namespace star
{

class DirectFileWriter {
    public:
	DirectFileWriter(const char *filename, std::size_t block_size, std::size_t emulated_persist_latency = 0)
		: block_size(block_size)
	{
                CHECK(emulated_persist_latency == 0);
		long flags = O_WRONLY | O_CREAT | O_TRUNC | O_DIRECT;
		fd = open(filename, flags, S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH | S_IWOTH);
		CHECK(fd >= 0);
	}

	~DirectFileWriter()
	{
	}

	void write(const char *str, long size)
	{
                ::write(fd, str, roundUp(size, block_size));
	}

	std::size_t roundUp(std::size_t numToRound, std::size_t multiple)
	{
		if (multiple == 0)
			return numToRound;

		int remainder = numToRound % multiple;
		if (remainder == 0)
			return numToRound;

		return numToRound + multiple - remainder;
	}

	void sync()
	{
		fdatasync(fd);
	}

	void close()
	{
		sync();
		int err = ::close(fd);
		CHECK(err == 0);
	}

    private:
	int fd;
	size_t block_size;
};

class BufferedDirectFileWriter {
    public:
	BufferedDirectFileWriter(const char *filename, std::size_t block_size, std::size_t emulated_persist_latency = 0)
		: block_size(block_size)
		, emulated_persist_latency(emulated_persist_latency)
	{
		long flags = O_WRONLY | O_CREAT | O_TRUNC;
		if (emulated_persist_latency == 0) {
			// Not using emulation, use O_DIRECT
			flags |= O_DIRECT;
		}
		fd = open(filename, flags, S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH | S_IWOTH);
		CHECK(fd >= 0);
		CHECK(BUFFER_SIZE % block_size == 0);
		bytes_total = 0;
		CHECK(posix_memalign((void **)&buffer, block_size, BUFFER_SIZE) == 0);
	}

	~BufferedDirectFileWriter()
	{
		free(buffer);
	}

	void write(const char *str, long size)
	{
		if (bytes_total + size < BUFFER_SIZE) {
			memcpy(buffer + bytes_total, str, size);
			bytes_total += size;
			return;
		}

		auto copy_size = BUFFER_SIZE - bytes_total;

		memcpy(buffer + bytes_total, str, copy_size);
		bytes_total += copy_size;
		flush();

		str += copy_size;
		size -= copy_size;

		if (size >= BUFFER_SIZE) {
			CHECK(false);
			int err = ::write(fd, str, size);
			CHECK(err >= 0);
			bytes_total = 0;
		} else {
			memcpy(buffer, str, size);
			bytes_total += size;
		}
	}

	std::size_t roundUp(std::size_t numToRound, std::size_t multiple)
	{
		if (multiple == 0)
			return numToRound;

		int remainder = numToRound % multiple;
		if (remainder == 0)
			return numToRound;

		return numToRound + multiple - remainder;
	}

	std::size_t flush()
	{
		DCHECK(fd >= 0);
		std::size_t s = bytes_total;
		if (bytes_total > 0) {
			std::size_t io_size = 0;
			int err;
			if (emulated_persist_latency != 0) {
				io_size = bytes_total;
			} else {
				io_size = roundUp(bytes_total, block_size);
				CHECK(io_size <= BUFFER_SIZE);
			}
			err = ::write(fd, buffer, io_size);
                        fdatasync(fd);
			int errnumber = errno;
			CHECK(err >= 0);
			CHECK(errnumber == 0);
		}
		bytes_total = 0;
		return s;
	}

	std::size_t sync()
	{
		DCHECK(fd >= 0);
		int err = 0;
		std::size_t s;
		if (emulated_persist_latency != 0) {
			s = flush();
			std::this_thread::sleep_for(std::chrono::microseconds(emulated_persist_latency));
		} else {
			s = flush();
		}
		CHECK(err == 0);
		return s;
	}

	void close()
	{
		flush();
		int err = ::close(fd);
		CHECK(err == 0);
	}

    public:
	static constexpr uint32_t BUFFER_SIZE = 1024 * 1024 * 4; // 4MB

    private:
	int fd;
	size_t block_size;
	char *buffer;
	std::size_t bytes_total;
	std::size_t emulated_persist_latency;
};

class WALLogger {
    public:
	WALLogger(const std::string &filename, std::size_t emulated_persist_latency)
		: filename(filename)
		, emulated_persist_latency(emulated_persist_latency)
	{
	}

	virtual ~WALLogger()
	{
	}

	virtual size_t write(
		const char *str, long size, bool persist, std::chrono::steady_clock::time_point txn_start_time, std::function<void()> on_blocking = []() {}) = 0;
	virtual void sync(
		size_t lsn, std::function<void()> on_blocking = []() {}) = 0;
	virtual void close() = 0;

	virtual void print_sync_stats(){};

	const std::string filename;
	std::size_t emulated_persist_latency;
};

class BlackholeLogger : public WALLogger {
    public:
	BlackholeLogger(const std::string &filename, std::size_t emulated_persist_latency = 0, std::size_t block_size = 4096)
		: WALLogger(filename, emulated_persist_latency)
		, writer(filename.c_str(), block_size, emulated_persist_latency)
	{
	}

	~BlackholeLogger() override
	{
	}

	size_t write(
		const char *str, long size, bool persist, std::chrono::steady_clock::time_point txn_start_time, std::function<void()> on_blocking = []() {}) override
	{
		return 0;
	}
	void sync (
		size_t lsn, std::function<void()> on_blocking = []() {}) override
	{
		return;
	}

	void close() override
	{
		writer.close();
	}

	BufferedDirectFileWriter writer;
};

class GroupCommitLogger : public WALLogger {
    public:
	GroupCommitLogger(const std::string &filename, std::size_t group_commit_txn_cnt, std::size_t group_commit_latency = 10,
			  std::size_t emulated_persist_latency = 0, std::size_t block_size = 4096)
		: WALLogger(filename, emulated_persist_latency)
		, writer(filename.c_str(), block_size, emulated_persist_latency)
		, write_lsn(0)
		, sync_lsn(0)
		, group_commit_latency_us(group_commit_latency)
		, group_commit_txn_cnt(group_commit_txn_cnt)
		, last_sync_time(Time::now())
		, waiting_syncs(0)
	{
		std::thread([this]() {
			while (true) {
				if (waiting_syncs.load() >= this->group_commit_txn_cnt || (Time::now() - last_sync_time) / 1000 >= group_commit_latency_us) {
					do_sync();
				}
				std::this_thread::sleep_for(std::chrono::microseconds(2));
			}
		}).detach();
	}

	~GroupCommitLogger() override
	{
	}

	std::size_t write(
		const char *str, long size, bool persist, std::chrono::steady_clock::time_point txn_start_time, std::function<void()> on_blocking = []() {}) override
	{
		uint64_t end_lsn;
		{
			std::lock_guard<std::mutex> g(mtx);
			auto start_lsn = write_lsn.load();
			end_lsn = start_lsn + size;
			writer.write(str, size);
			write_lsn += size;
		}
		if (persist) {
			sync(end_lsn, on_blocking);
		}
		return end_lsn;
	}

	void do_sync()
	{
		std::lock_guard<std::mutex> g(mtx);
		auto waiting_sync_cnt = waiting_syncs.load();
		if (waiting_sync_cnt < group_commit_txn_cnt && (Time::now() - last_sync_time) / 1000 < group_commit_latency_us) {
			return;
		}

		auto flush_lsn = write_lsn.load();
		waiting_sync_cnt = waiting_syncs.load();

		if (sync_lsn < write_lsn) {
			auto t = Time::now();
			sync_batch_bytes.add(writer.sync());
			sync_time.add(Time::now() - t);
			// LOG(INFO) << "sync " << waiting_sync_cnt << " writes";
			grouping_time.add(Time::now() - last_sync_time);
			sync_batch_size.add(waiting_sync_cnt);
		}
		last_sync_time = Time::now();
		waiting_syncs -= waiting_sync_cnt;
		sync_lsn.store(flush_lsn);
	}

	void sync(
		std::size_t lsn, std::function<void()> on_blocking = []() {}) override
	{
		waiting_syncs.fetch_add(1);
		while (sync_lsn.load() < lsn) {
			on_blocking();
			// std::this_thread::sleep_for(std::chrono::microseconds(10));
		}
	}

	void close() override
	{
		writer.close();
	}

	void print_sync_stats() override
	{
		LOG(INFO) << "Log Hardening Time " << this->sync_time.nth(50) / 1000 << " us (50th), " << this->sync_time.nth(75) / 1000 << " us (75th), "
			  << this->sync_time.nth(90) / 1000 << " us (90th), " << this->sync_time.nth(95) / 1000 << " us (95th). "
			  << "Log Hardenning Batch Size " << this->sync_batch_size.nth(50) << " (50th), " << this->sync_batch_size.nth(75) << " (75th), "
			  << this->sync_batch_size.nth(90) << " (90th), " << this->sync_batch_size.nth(95) << " (95th). "
			  << "Log Hardenning Bytes " << this->sync_batch_bytes.nth(50) << " (50th), " << this->sync_batch_bytes.nth(75) << " (75th), "
			  << this->sync_batch_bytes.nth(90) << " (90th), " << this->sync_batch_bytes.nth(95) << " (95th). "
			  << "Log Grouping Time " << this->grouping_time.nth(50) / 1000 << " us (50th), " << this->grouping_time.nth(75) / 1000
			  << " us (75th), " << this->grouping_time.nth(90) / 1000 << " us (90th), " << this->grouping_time.nth(95) / 1000 << " us (95th). ";
	}

    private:
	std::mutex mtx;
	BufferedDirectFileWriter writer;
	std::atomic<uint64_t> write_lsn;
	std::atomic<uint64_t> sync_lsn;
	std::size_t group_commit_latency_us;
	std::size_t group_commit_txn_cnt;
	std::atomic<std::size_t> last_sync_time;
	std::atomic<uint64_t> waiting_syncs;
	Percentile<uint64_t> sync_time;
	Percentile<uint64_t> grouping_time;
	Percentile<uint64_t> sync_batch_size;
	Percentile<uint64_t> sync_batch_bytes;
};

struct LogBuffer {
        static constexpr uint64_t max_buffer_size = 1024 * 1024 * 4;

        char buffer[max_buffer_size];
        uint64_t size = 0;

        // statistics
        std::vector<uint64_t> txn_start_times;
};

static constexpr uint64_t max_log_buffer_queue_size = 128;
using LockfreeLogBufferQueue = LockfreeQueue<LogBuffer *, max_log_buffer_queue_size>;

class PashaGroupCommitLoggerSlave : public WALLogger {
    public:
	PashaGroupCommitLoggerSlave(LockfreeLogBufferQueue *log_buffer_queue_ptr, std::atomic<uint64_t> *cxl_global_epoch)
		: WALLogger("nothing", 0)
                , log_buffer_queue(*log_buffer_queue_ptr)
                , cxl_global_epoch(cxl_global_epoch)
	{
                cur_log_buffer = new LogBuffer;
                CHECK(cur_log_buffer != nullptr);
	}

	~PashaGroupCommitLoggerSlave() override
	{
                delete cur_log_buffer;
	}

	std::size_t write(const char *str, long size, bool persist, std::chrono::steady_clock::time_point txn_start_time, std::function<void()> on_blocking = []() {}) override
	{
                uint64_t cur_epoch = cxl_global_epoch->load();

                CHECK(cur_log_buffer != nullptr);

                if (((cur_log_buffer->size + size) > LogBuffer::max_buffer_size) || (cur_epoch > last_epoch)) {
                        if (cur_log_buffer->size > 0) {
                                log_buffer_queue.push(cur_log_buffer);
                                cur_log_buffer = new LogBuffer;
                                CHECK(cur_log_buffer != nullptr);
                        }
                        last_epoch = cur_epoch;
                }

                memcpy(&cur_log_buffer->buffer[cur_log_buffer->size], str, size);
                cur_log_buffer->size += size;

                if (persist == true) {
                        auto latency = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - txn_start_time).count();
                        cur_log_buffer->txn_start_times.push_back(Time::now() - latency);
                }

                return size;
	}

        void sync(std::size_t lsn, std::function<void()> on_blocking = []() {}) override
	{
		CHECK(0);
	}

	void close() override
	{
	}

    private:
        LogBuffer *cur_log_buffer{ nullptr };
	LockfreeLogBufferQueue &log_buffer_queue;
        std::atomic<uint64_t> *cxl_global_epoch{ nullptr };
        uint64_t last_epoch{ 0 };
};

class PashaGroupCommitLogger : public WALLogger {
    public:
	PashaGroupCommitLogger(const std::string &filename, std::vector<LockfreeLogBufferQueue *> *log_buffer_queues_ptr,
                          std::atomic<uint64_t> *cxl_global_epoch, std::atomic<bool> &stopFlag,
                          std::size_t group_commit_txn_cnt, std::size_t group_commit_latency = 10,
			  std::size_t emulated_persist_latency = 0, std::size_t block_size = 4096)
		: WALLogger(filename, emulated_persist_latency)
                , file_writer(filename.c_str(), block_size, emulated_persist_latency)
                , log_buffer_queues(*log_buffer_queues_ptr)
                , cxl_global_epoch(cxl_global_epoch)
                , group_commit_latency_us(group_commit_latency)
                , disk_sync_cnt(0)
                , disk_sync_size(0)
                , committed_txn_cnt(0)
		, last_sync_time(Time::now())
                , stopFlag(stopFlag)
	{
                CHECK(emulated_persist_latency == 0);
	}

	~PashaGroupCommitLogger() override
	{
	}

        void start()
        {
                LOG(INFO) << "logger thread started!";
                while (stopFlag.load() == false) {
                        if ((Time::now() - last_sync_time) / 1000 >= group_commit_latency_us) {
                                this->cxl_global_epoch->fetch_add(1);
                                do_sync();
                                last_sync_time = Time::now();
                        }
                        std::this_thread::sleep_for(std::chrono::microseconds(2));
                }
        }

	std::size_t write(const char *str, long size, bool persist, std::chrono::steady_clock::time_point txn_start_time, std::function<void()> on_blocking = []() {}) override
	{
                CHECK(0);
	}

	void do_sync()
	{
                LockfreeLogBufferQueue *cur_log_buffer_queue = nullptr;
                auto begin_time = std::chrono::steady_clock::now();     // used for calculating queuing up time
                uint64_t flushed_log_buffer_num = 0;

                for (auto i = 0; i < log_buffer_queues.size(); i++) {
                        cur_log_buffer_queue = log_buffer_queues[i];
                        while (true) {
                                if (cur_log_buffer_queue->empty() == true) {
                                        break;
                                }

                                // get the buffer
                                LogBuffer *log_buffer = cur_log_buffer_queue->front();
                                CHECK(log_buffer != nullptr);

                                // release the slot
                                cur_log_buffer_queue->pop();

                                // write to disk
                                auto sync_start_time = std::chrono::steady_clock::now();
                                file_writer.write(log_buffer->buffer, log_buffer->size);
                                file_writer.sync();
                                auto sync_end_time = std::chrono::steady_clock::now();

                                // calculate queuing up time
                                auto queuing_up_time = std::chrono::duration_cast<std::chrono::microseconds>(sync_start_time - begin_time).count();
                                queuing_latency.add(queuing_up_time);

                                // collect disk sync stats
                                auto sync_latency = std::chrono::duration_cast<std::chrono::microseconds>(sync_end_time - sync_start_time).count();
                                disk_sync_latency.add(sync_latency);
                                disk_sync_cnt++;
                                disk_sync_size += log_buffer->size;

                                // calculate transaction latency and collect stats
                                committed_txn_cnt += log_buffer->txn_start_times.size();
                                auto now = Time::now();
                                for (auto i = 0; i < log_buffer->txn_start_times.size(); i++) {
                                        auto latency = now - log_buffer->txn_start_times[i];
                                        txn_latency.add(latency / 1000);
                                }

                                // refresh begin time here because VM has clock drift between threads
                                flushed_log_buffer_num++;
                                if (flushed_log_buffer_num >= (max_log_buffer_queue_size * log_buffer_queues.size())) {
                                        begin_time = std::chrono::steady_clock::now();
                                }

                                delete log_buffer;
                        }
                }
	}

	void sync(std::size_t lsn, std::function<void()> on_blocking = []() {}) override
	{
		CHECK(0);
	}

	void close() override
	{
                file_writer.close();
	}

        void print_sync_stats() override
	{
                LOG(INFO) << "Group Commit Stats: "
                          << txn_latency.nth(50) << " us (50%) " << txn_latency.nth(75) << " us (75%) "
                          << txn_latency.nth(95) << " us (95%) " << txn_latency.nth(99) << " us (99%) "
			  << txn_latency.avg() << " us (avg)"
                          << " committed_txn_cnt " << committed_txn_cnt;

                LOG(INFO) << "Queuing Stats: "
                          << queuing_latency.nth(50) << " us (50%) " << queuing_latency.nth(75) << " us (75%) "
                          << queuing_latency.nth(95) << " us (95%) " << queuing_latency.nth(99) << " us (99%) "
			  << queuing_latency.avg() << " us (avg)";

                LOG(INFO) << "Disk Sync Stats: "
                          << disk_sync_latency.nth(50) << " us (50%) " << disk_sync_latency.nth(75) << " us (75%) "
                          << disk_sync_latency.nth(95) << " us (95%) " << disk_sync_latency.nth(99) << " us (99%) "
			  << disk_sync_latency.avg() << " us (avg)"
                          << " disk_sync_cnt " << disk_sync_cnt
                          << " disk_sync_size " << disk_sync_size
                          << " current global epoch " << this->cxl_global_epoch->load();
	}

    private:
	std::mutex mutex;
        DirectFileWriter file_writer;
	std::vector<LockfreeLogBufferQueue *> &log_buffer_queues;
        std::atomic<uint64_t> *cxl_global_epoch{ nullptr };
	std::size_t group_commit_latency_us{ 0 };

        // statistics
        Percentile<uint64_t> queuing_latency;
        uint64_t disk_sync_cnt{ 0 };
        uint64_t disk_sync_size{ 0 };
        Percentile<uint64_t> disk_sync_latency;
        uint64_t committed_txn_cnt{ 0 };
        Percentile<uint64_t> txn_latency;

	std::atomic<std::size_t> last_sync_time{ 0 };
        std::atomic<bool> &stopFlag;
};

class SimpleWALLogger : public WALLogger {
    public:
	SimpleWALLogger(const std::string &filename, std::size_t emulated_persist_latency = 0, std::size_t block_size = 4096)
		: WALLogger(filename, emulated_persist_latency)
		, writer(filename.c_str(), block_size, emulated_persist_latency)
	{
	}

	~SimpleWALLogger() override
	{
	}

	std::size_t write(
		const char *str, long size, bool persist, std::chrono::steady_clock::time_point txn_start_time, std::function<void()> on_blocking = []() {}) override
	{
		std::lock_guard<std::mutex> g(mtx);
		writer.write(str, size);
		if (persist) {
			writer.sync();
		}
		return 0;
	}

	void sync(
		std::size_t lsn, std::function<void()> on_blocking = []() {}) override
	{
		std::lock_guard<std::mutex> g(mtx);
		writer.sync();
	}

	void close() override
	{
		std::lock_guard<std::mutex> g(mtx);
		writer.close();
	}

    private:
	BufferedDirectFileWriter writer;
	std::mutex mtx;
};

// class ScalableWALLogger : public WALLogger {
// public:

//   ScalableWALLogger(const std::string & filename, std::size_t emulated_persist_latency = 0)
//     : WALLogger(filename, emulated_persist_latency), write_lsn(0), alloc_lsn(0), emulated_persist_latency(emulated_persist_latency){
//     fd = open(filename.c_str(), O_WRONLY | O_CREAT | O_TRUNC,
//               S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH | S_IWOTH);
//   }

//   ~ScalableWALLogger() override {}

//   std::size_t roundUp(std::size_t numToRound, std::size_t multiple)
//   {
//       DCHECK(multiple);
//       int isPositive = 1;
//       return ((numToRound + isPositive * (multiple - 1)) / multiple) * multiple;
//   }

//   std::size_t write(const char *str, long size) override{
//     auto write_seq = alloc_lsn.load();
//     auto start_lsn = alloc_lsn.fetch_add(size);

//     while (start_lsn - flush_lsn > BUFFER_SIZE) {
//       // Wait for previous buffer to be flush down to file system (page cache).
//       std::this_thread::sleep_for(std::chrono::microseconds(1));
//     }

//     auto end_lsn = start_lsn + size;
//     auto start_buffer_offset = write_seq % BUFFER_SIZE;
//     auto buffer_left_size = BUFFER_SIZE - start_buffer_offset;

//     if (buffer_left_size >= size) {
//       memcpy(buffer + start_buffer_offset, str, size);
//       write_lsn += size;
//       buffer_left_size -= size;
//       size = 0;
//     } else {
//       memcpy(buffer + start_buffer_offset, str, buffer_left_size);
//       write_lsn += buffer_left_size;
//       size -= buffer_left_size;
//       buffer_left_size = 0;
//       str += buffer_left_size;
//     }

//     if (size || buffer_left_size == 0) { // torn write, flush data
//       auto block_up_seq = roundUp(write_seq, BUFFER_SIZE);
//       if (write_seq % BUFFER_SIZE == 0) {
//         block_up_seq = write_seq + BUFFER_SIZE;
//       }

//       while (write_lsn.load() == block_up_seq) { // Wait for the holes in the current block to be filled
//         std::this_thread::sleep_for(std::chrono::microseconds(1));
//       }
//       auto flush_lsn_save = flush_lsn.load();
//       {
//         std::lock_guard<std::mutex> g(mtx);
//         if (flush_lsn_save == flush_lsn) {// Only allow one thread to acquire the right to flush down the buffer
//           int err = ::write(fd, buffer, BUFFER_SIZE);
//           CHECK(err >= 0);
//           flush_lsn.fetch_add(BUFFER_SIZE);
//           DCHECK(block_up_seq == flush_lsn);
//         }
//       }

//       if (size) { // Write the left part to the new buffer
//         memcpy(buffer + 0, str, size);
//         write_lsn += size;
//       }
//     }

//     return end_lsn;
//   }

//   void sync_file() {
//     DCHECK(fd >= 0);
//     int err = 0;
//     if (emulated_persist_latency)
//       std::this_thread::sleep_for(std::chrono::microseconds(emulated_persist_latency));
//     //err = fdatasync(fd);
//     CHECK(err == 0);
//   }

//   void sync(std::size_t lsn) override {
//     DCHECK(fd >= 0);
//     int err = 0;
//     while (sync_lsn < lsn) {

//     }
//     sync_lsn = flush_lsn.load();
//     if (emulated_persist_latency)
//       std::this_thread::sleep_for(std::chrono::microseconds(emulated_persist_latency));
//     //err = fdatasync(fd);
//     CHECK(err == 0);
//   }

//   void close() override {
//     ::close(fd);
//   }

// private:
//   int fd;
//   std::mutex mtx;

// public:
//   static constexpr uint32_t BUFFER_SIZE = 1024 * 1024 * 4; // 4MB

// private:
//   std::atomic<uint64_t> alloc_lsn;
//   std::atomic<uint64_t> write_lsn;
//   std::atomic<uint64_t> flush_lsn;
//   std::atomic<uint64_t> sync_lsn;
//   char buffer[BUFFER_SIZE];
//   std::size_t emulated_persist_latency;
// };

}
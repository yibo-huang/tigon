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

#include "BufferedFileWriter.h"
#include "Time.h"
namespace star {


class BufferedDirectFileWriter {

public:
  BufferedDirectFileWriter(const char *filename, std::size_t block_size, std::size_t emulated_persist_latency = 0) 
    : block_size(block_size), emulated_persist_latency(emulated_persist_latency) {
    long flags = O_WRONLY | O_CREAT | O_TRUNC;
    if (emulated_persist_latency == 0) {
      // Not using emulation, use O_DIRECT
      flags |= O_DIRECT;
    }
    fd = open(filename, flags, S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH | S_IWOTH);
    CHECK(fd >= 0);
    CHECK(BUFFER_SIZE % block_size == 0);
    bytes_total = 0;
    CHECK(posix_memalign((void**)&buffer, block_size, BUFFER_SIZE) == 0);
  }

  ~BufferedDirectFileWriter() {
    free(buffer);
  }

  void write(const char *str, long size) {

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

  std::size_t flush() {
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
      int errnumber = errno;
      CHECK(err >= 0);
      CHECK(errnumber == 0);
    }
    bytes_total = 0;
    return s;
  }

  std::size_t sync() {
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

  void close() {
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
  WALLogger(const std::string & filename, std::size_t emulated_persist_latency) : filename(filename), emulated_persist_latency(emulated_persist_latency) {}

  virtual ~WALLogger() {}

  virtual size_t write(const char *str, long size, bool persist, std::function<void()> on_blocking = [](){}) = 0;
  virtual void sync(size_t lsn, std::function<void()> on_blocking = [](){}) = 0;
  virtual void close() = 0;

  virtual void print_sync_stats()  {};

  const std::string filename;
  std::size_t emulated_persist_latency;
};

class BlackholeLogger : public WALLogger {
public:
  BlackholeLogger(const std::string & filename, std::size_t emulated_persist_latency = 0, std::size_t block_size = 4096) 
    : WALLogger(filename, emulated_persist_latency), writer(filename.c_str(), 
    block_size, emulated_persist_latency){}
  ~BlackholeLogger() override {}

  size_t write(const char *str, long size, bool persist, std::function<void()> on_blocking = [](){}) override {
    return 0;
  }
  void sync(size_t lsn, std::function<void()> on_blocking = [](){}) {
    return;
  }

  void close() {
    writer.close();
  }

  BufferedDirectFileWriter writer;
};

class GroupCommitLogger : public WALLogger {
public:

  GroupCommitLogger(const std::string & filename, std::size_t group_commit_txn_cnt, std::size_t group_commit_latency = 10, std::size_t emulated_persist_latency = 0, std::size_t block_size = 4096) 
    : WALLogger(filename, emulated_persist_latency), writer(filename.c_str(), 
    block_size, emulated_persist_latency), write_lsn(0), sync_lsn(0), 
    group_commit_latency_us(group_commit_latency), 
    group_commit_txn_cnt(group_commit_txn_cnt), last_sync_time(Time::now()), waiting_syncs(0) {
    std::thread([this](){
      while (true) {
        if (waiting_syncs.load() >= this->group_commit_txn_cnt || (Time::now() - last_sync_time) / 1000 >= group_commit_latency_us) {
          do_sync();
        }
        std::this_thread::sleep_for(std::chrono::microseconds(2));
      }
    }).detach();
  }

  ~GroupCommitLogger() override {}

  std::size_t write(const char *str, long size, bool persist, std::function<void()> on_blocking = [](){}) override{
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

  void do_sync() {
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
      //LOG(INFO) << "sync " << waiting_sync_cnt << " writes"; 
      grouping_time.add(Time::now() - last_sync_time);
      sync_batch_size.add(waiting_sync_cnt);
    }
    last_sync_time = Time::now();
    waiting_syncs -= waiting_sync_cnt;
    sync_lsn.store(flush_lsn);
  }

  void sync(std::size_t lsn, std::function<void()> on_blocking = [](){}) override {
    waiting_syncs.fetch_add(1);
    while (sync_lsn.load() < lsn) {
      on_blocking();
      //std::this_thread::sleep_for(std::chrono::microseconds(10));
    }
  }

  void close() override {
    writer.close();
  }

  void print_sync_stats() override {
    LOG(INFO) << "Log Hardening Time "
              << this->sync_time.nth(50) / 1000 << " us (50th), "
              << this->sync_time.nth(75) / 1000 << " us (75th), "
              << this->sync_time.nth(90) / 1000 << " us (90th), "
              << this->sync_time.nth(95) / 1000 << " us (95th). "
              << "Log Hardenning Batch Size "
              << this->sync_batch_size.nth(50) << " (50th), "
              << this->sync_batch_size.nth(75) << " (75th), "
              << this->sync_batch_size.nth(90) << " (90th), "
              << this->sync_batch_size.nth(95) << " (95th). "
               << "Log Hardenning Bytes "
              << this->sync_batch_bytes.nth(50) << " (50th), "
              << this->sync_batch_bytes.nth(75) << " (75th), "
              << this->sync_batch_bytes.nth(90) << " (90th), "
              << this->sync_batch_bytes.nth(95) << " (95th). "
              << "Log Grouping Time "
              << this->grouping_time.nth(50) / 1000 << " us (50th), "
              << this->grouping_time.nth(75) / 1000 << " us (75th), "
              << this->grouping_time.nth(90) / 1000 << " us (90th), "
              << this->grouping_time.nth(95) / 1000 << " us (95th). ";
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


class SimpleWALLogger : public WALLogger {
public:

  SimpleWALLogger(const std::string & filename, std::size_t emulated_persist_latency = 0, std::size_t block_size = 4096) 
    : WALLogger(filename, emulated_persist_latency), writer(filename.c_str(), block_size, emulated_persist_latency) {
  }

  ~SimpleWALLogger() override {}
  std::size_t write(const char *str, long size, bool persist, std::function<void()> on_blocking = [](){}) override{
    std::lock_guard<std::mutex> g(mtx);
    writer.write(str, size);
    if (persist) {
      writer.sync();
    }
    return 0;
  }

  void sync(std::size_t lsn, std::function<void()> on_blocking = [](){}) override {
    std::lock_guard<std::mutex> g(mtx);
    writer.sync();
  }

  void close() override {
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
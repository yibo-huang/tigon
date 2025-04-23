//
// Epoch-based memory reclaimation across a CXL pod
//

#pragma once

#include "stdint.h"
#include <glog/logging.h>
#include <boost/interprocess/offset_ptr.hpp>

#include "common/CXLMemory.h"

namespace star
{

class CXL_EBR {
    public:
        static constexpr uint64_t max_ebr_retiring_memory = 1 * 1024 * 1024;    // 1MB

        // 0 - max_epoch
        static constexpr uint64_t max_epoch = 3;

        static constexpr uint64_t max_coordinator_num = 8;
        static constexpr uint64_t max_thread_num = 5;

        // try to advance global epoch when we have more than this number of garbage
        static constexpr uint64_t epoch_advance_threshold = 100;

        struct retired_object {
                retired_object(void *ptr, uint64_t size, uint64_t category)
                        : ptr(ptr)
                        , size(size)
                        , category(category)
                {}

                void *ptr;
                uint64_t size;
                uint64_t category;
        };

        // per-thread EBR metadata in local DRAM
        struct EBRMetaLocal {
                uint64_t coordinator_id;
                uint64_t thread_id;

                uint64_t last_freed_epoch;

                std::vector<retired_object> retired_objects[max_epoch];

                // statistics
                Percentile<uint64_t> garbage_size;
                uint64_t max_garbage_size;
        };

        // per-thread EBR metadata in CXL
        struct EBRMetaCXL {
                std::atomic<uint64_t> local_epoch{ 0 }; // local epoch always <= global epoch
        };

        CXL_EBR(uint64_t coordinator_num, uint64_t thread_num)
                : coordinator_num(coordinator_num)
                , thread_num(thread_num)
        {
        }

        void thread_init_ebr_meta(uint64_t coordinator_id, uint64_t thread_id)
        {
                static std::atomic<uint64_t> thread_id_candidate{ 0 };

                EBRMetaLocal &local_ebr_meta = get_local_ebr_meta();

                local_ebr_meta.coordinator_id = coordinator_id;
                local_ebr_meta.thread_id = thread_id_candidate++;

                local_ebr_meta.last_freed_epoch = 0;

                for (uint64_t i = 0; i < max_epoch; i++) {
                        local_ebr_meta.retired_objects[i].clear();
                }

                local_ebr_meta.garbage_size.clear();
                local_ebr_meta.max_garbage_size = 0;

                LOG(INFO) << "init local EBR metadata, coordinator_id = " << local_ebr_meta.coordinator_id << " thread_id = " << local_ebr_meta.thread_id;
        }

        void add_retired_object(void *ptr, uint64_t size, uint64_t category)
        {
                EBRMetaLocal &local_ebr_meta = get_local_ebr_meta();
                uint64_t coordinator_id = local_ebr_meta.coordinator_id;
                uint64_t thread_id = local_ebr_meta.thread_id;

                EBRMetaCXL &cxl_ebr_meta = cxl_ebr_meta_vec[coordinator_id][thread_id];
                uint64_t cur_local_epoch = cxl_ebr_meta.local_epoch.load(std::memory_order_acquire);

                // add the object to the list of the current local epoch
                std::vector<retired_object> &cur_retired_object_list = local_ebr_meta.retired_objects[cur_local_epoch % max_epoch];
                retired_object object(ptr, size, category);
                cur_retired_object_list.push_back(object);
        }

        void enter_critical_section()
        {
                EBRMetaLocal &local_ebr_meta = get_local_ebr_meta();
                uint64_t coordinator_id = local_ebr_meta.coordinator_id;
                uint64_t thread_id = local_ebr_meta.thread_id;

                EBRMetaCXL &cxl_ebr_meta = cxl_ebr_meta_vec[coordinator_id][thread_id];
                uint64_t cur_local_epoch = cxl_ebr_meta.local_epoch.load(std::memory_order_acquire);

                // load global epoch
                uint64_t cur_global_epoch = global_epoch.load(std::memory_order_acquire);

                if (cur_global_epoch == cur_local_epoch) {
                        std::vector<retired_object> &cur_retired_object_list = local_ebr_meta.retired_objects[cur_local_epoch % max_epoch];

                        // try to advance the global epoch
                        if (cur_retired_object_list.size() >= epoch_advance_threshold) {
                                bool advance_global_ebr = true;

                                // check if all other threads have entered the current epoch
                                for (uint64_t i = 0; i < coordinator_num; i++) {
                                        for (uint64_t j = 0; j < thread_num; j++) {
                                                uint64_t local_epoch = cxl_ebr_meta_vec[i][j].local_epoch.load(std::memory_order_acquire);
                                                if (local_epoch < cur_global_epoch) {   // local epoch might be larger than 'cur_global_epoch' because of race conditions
                                                        advance_global_ebr = false;
                                                        break;
                                                }
                                        }
                                }

                                // advance the global epoch
                                if (advance_global_ebr == true) {
                                        uint64_t new_global_epoch = cur_global_epoch + 1;
                                        global_epoch.compare_exchange_strong(cur_global_epoch, new_global_epoch, std::memory_order_acq_rel);
                                }
                        }
                } else {
                        CHECK(cur_global_epoch > cur_local_epoch);
                }

                // reload global epoch
                cur_global_epoch = global_epoch.load(std::memory_order_acquire);

                // update local epoch if necessary
                if (cur_local_epoch < cur_global_epoch) {
                        CHECK(cur_local_epoch == cur_global_epoch - 1);
                        cxl_ebr_meta.local_epoch.store(cur_global_epoch, std::memory_order_release);
                }

                // now it is time to reclaim garbage in local_epoch - 2
                if (cur_global_epoch >= 2) {
                        uint64_t epoch_to_reclaim = cur_global_epoch - 2;
                        if (epoch_to_reclaim > local_ebr_meta.last_freed_epoch) {
                                CHECK(epoch_to_reclaim == local_ebr_meta.last_freed_epoch + 1);
                                uint64_t gc_size = 0;
                                std::vector<retired_object> &retired_object_list_to_reclaim = local_ebr_meta.retired_objects[epoch_to_reclaim % max_epoch];

                                for (uint64_t i = 0; i < retired_object_list_to_reclaim.size(); i++) {
                                        cxlalloc_free(retired_object_list_to_reclaim[i].ptr);
                                        gc_size += retired_object_list_to_reclaim[i].size;
                                }

                                local_ebr_meta.garbage_size.add(gc_size);
                                if (gc_size > local_ebr_meta.max_garbage_size) {
                                        local_ebr_meta.max_garbage_size = gc_size;
                                }
                                local_ebr_meta.last_freed_epoch = epoch_to_reclaim;
                                retired_object_list_to_reclaim.clear();
                        }
                }
        }

        void leave_critical_section()
        {
                CHECK(0);       // not used
        }

        void print_statistics()
        {
                EBRMetaLocal &local_ebr_meta = get_local_ebr_meta();

                LOG(INFO) << "EBR Statistics for worker " << local_ebr_meta.thread_id
                        << ": GC size 100% = " << local_ebr_meta.garbage_size.nth(100)
                        << " 99% = " << local_ebr_meta.garbage_size.nth(99)
                        << " 50% = " << local_ebr_meta.garbage_size.nth(50)
                        << " avg = " << local_ebr_meta.garbage_size.avg()
                        << " max = " << local_ebr_meta.max_garbage_size;
        }

    private:
        static EBRMetaLocal &get_local_ebr_meta()
	{
		static thread_local EBRMetaLocal local_ebr_meta;
		return local_ebr_meta;
	}

        uint64_t coordinator_num{ 0 };
        uint64_t thread_num{ 0 };

        std::atomic<uint64_t> global_epoch{ 0 };

        EBRMetaCXL cxl_ebr_meta_vec[max_coordinator_num][max_thread_num];
};

extern CXL_EBR *global_ebr_meta;

} // namespace star

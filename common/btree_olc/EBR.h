#pragma once
#include <assert.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <iostream>
#include <list>
#include <utility>

constexpr unsigned kNumberEpochs = 3;

template <unsigned Epochs>
struct Orphan {
    Orphan(unsigned target_epoch, std::array<std::list<void *>, Epochs> &retire_lists)
        : target_epoch_(target_epoch), retire_lists_(retire_lists), next_(nullptr) {}

    ~Orphan() {}

    const unsigned target_epoch_;

    std::array<std::list<void *>, Epochs> retire_lists_;

    Orphan<Epochs> *next_;
};

template <std::size_t UpdateThreshold, class Deallocator>
class EBR {
   private:
    EBR() {}

   public:
    struct ThreadControlBlock {
        ThreadControlBlock()
            : is_in_critical_region_(false),
              local_epoch_(kNumberEpochs),
              next_entry_(nullptr),
              in_use_(true) {}

        bool tryAdopt() {
            if (!in_use_.load(std::memory_order_relaxed)) {
                bool expected = false;
                return in_use_.compare_exchange_strong(expected, true, std::memory_order_acquire);
            }
            return false;
        }

        bool isActive() const { return in_use_.load(std::memory_order_relaxed); }

        void abandon() { in_use_.store(false, std::memory_order_release); }

        std::atomic<bool> is_in_critical_region_;
        std::atomic<unsigned> local_epoch_;
        ThreadControlBlock *next_entry_;
        std::atomic<bool> in_use_;
    };

    struct ThreadData {
        ~ThreadData() {
            // no control_block_ -> nothing to do
            if (control_block_ == nullptr) return;

            // we can avoid creating an orphan in case we have no retired nodes left.
            if (std::any_of(retire_lists_.begin(), retire_lists_.end(),
                            [](std::list<void *> p) { return p.size() != 0; })) {
                // global_epoch_ - 1 (mod kNumberEpochs) guarantees a full cycle, making sure no
                // other thread may still have a reference to an object in one of the retire lists.
                auto target_epoch =
                    (global_epoch_.load(std::memory_order_relaxed) + kNumberEpochs - 1) %
                    kNumberEpochs;
                assert(target_epoch < kNumberEpochs);
                // unreclaimed nodes are placed into global_thread_block_list_,
                // when an unexpected event happend
                global_thread_block_list_.abandon_retired_nodes(
                    new Orphan<kNumberEpochs>(target_epoch, retire_lists_));
            }
            // assert(control_block_->is_in_critical_region_.load(std::memory_order_relaxed) == false);
            global_thread_block_list_.release_entry(control_block_);
        }

        /**
         * when remove operation interrupt
         * the elements in path_stack need to exit
         */
        void batchLeave(unsigned num) {
            while (num > 0) {
                leaveCritical();
                num--;
            }
        }

        /**
         * thread enter critical region
         * prevent the global epoch from increasing too fast
         */
        void enterCritical() {
            if (++enter_count_ == 1) doEnterCritical();
        }

        void leaveCritical() {
            assert(enter_count_ > 0);
            if (--enter_count_ == 0) doLeaveCritical();
        }

        void addRetiredNode(void *p) {
                // TODO: EBR is kind of disabled now
                //     addRetiredNode(p, control_block_->local_epoch_.load(std::memory_order_relaxed));
        }

       private:
        void ensureHasControlBlock() {
            if (control_block_ == nullptr)
                control_block_ = global_thread_block_list_.acquire_entry();
        }

        /**
         * perform specific operations
         * like increase counters and try update global variables
         */
        void doEnterCritical() {
            ensureHasControlBlock();

            control_block_->is_in_critical_region_.store(true, std::memory_order_relaxed);
            //std::atomic_thread_fence(std::memory_order_seq_cst);

            auto epoch = global_epoch_.load(std::memory_order_acquire);

            if (control_block_->local_epoch_.load(std::memory_order_relaxed) != epoch) {
                entries_since_update_ = 0;
            } else if (entries_since_update_++ == UpdateThreshold) {
                entries_since_update_ = 0;
                const auto new_epoch = (epoch + 1) % kNumberEpochs;
                if (!tryUpdateEpoch(epoch, new_epoch)) return;
                epoch = new_epoch;
            } else {
                // local_epoch is new and can't update global epoch
                return;
            }

            // updated the global_epoch_ or observing a new epoch from
            control_block_->local_epoch_.store(epoch, std::memory_order_relaxed);
            for (auto p : retire_lists_[epoch]) {
                Deallocator{}(p);  // free the memory pointed to by `p`
            }
            retire_lists_[epoch].clear();
        }

        void doLeaveCritical() {
            control_block_->is_in_critical_region_.store(false, std::memory_order_release);
        }

        /**
         * put garbage node into retire_lists_
         * Be called when a node needs to be delete
         */
        void addRetiredNode(void *p, size_t epoch) {
            assert(epoch < kNumberEpochs);
            retire_lists_[epoch].push_back(p);
        }

        bool tryUpdateEpoch(unsigned curr_epoch, unsigned new_epoch) {
            const auto old_epoch = (curr_epoch + kNumberEpochs - 1) % kNumberEpochs;
            auto prevents_update = [old_epoch](const ThreadControlBlock &data) {
                return data.is_in_critical_region_.load(std::memory_order_relaxed) &&
                       data.local_epoch_.load(std::memory_order_relaxed) == old_epoch;
            };

            // If any thread hasn't advanced to the current epoch, abort the attempt.
            auto can_update = !std::any_of(global_thread_block_list_.begin(),
                                           global_thread_block_list_.end(), prevents_update);
            if (!can_update) return false;

            if (global_epoch_.load(std::memory_order_relaxed) == curr_epoch) {
                std::atomic_thread_fence(std::memory_order_acquire);

                bool success = global_epoch_.compare_exchange_strong(
                    curr_epoch, new_epoch, std::memory_order_release, std::memory_order_relaxed);

                if (success) adoptOrphans();
            }

            return true;
        }

        void adoptOrphans() {
            Orphan<kNumberEpochs> *cur =
                global_thread_block_list_.adopt_abandoned_retired_nodes();

            for (Orphan<kNumberEpochs> *next = nullptr; cur != nullptr; cur = next) {
                next = cur->next_;
                cur->next_ = nullptr;
                for (unsigned int i = 0; i < kNumberEpochs; i++) {
                    for (auto node : cur->retire_lists_[i]) {
                        addRetiredNode(node, cur->target_epoch_);
                    }
                }
                // cur->retire_lists_.clear();
                delete cur;
            }
        }

        unsigned enter_count_ = 0;

        // used to keep track of the number of entries
        // avoid updates global epoch too frequently
        unsigned entries_since_update_ = 0;
        ThreadControlBlock *control_block_ = nullptr;
        std::array<std::list<void *>, kNumberEpochs> retire_lists_ = {};
    };
    template <class T>
    class ThreadBlockList {
       public:
        ~ThreadBlockList() {
            auto h = abandoned_retired_nodes_.load();
            while (h != nullptr) {
                for (unsigned int i = 0; i < kNumberEpochs; i++) {
                    for (auto p : h->retire_lists_[i]) {
                        Deallocator{}(p);  // free the memory pointed to by `p`
                    }
                    h->retire_lists_[i].clear();
                }
                auto t = h;
                h = h->next_;
                delete t;
            }
            abandoned_retired_nodes_.store(h);
            auto hh = head_.load();
            while (hh) {
                auto t = hh->next_entry_;
                delete hh;
                hh = t;
            }
        }
        struct entry {
            entry() : in_use_(true), next_entry_(nullptr) {}

            bool is_active() const { return in_use_.load(std::memory_order_relaxed); }

            void abandon() { in_use_.store(false, std::memory_order_release); }

           private:
            friend class ThreadBlockList;

            bool try_adopt() {
                if (!in_use_.load(std::memory_order_relaxed)) {
                    bool expected = false;
                    return in_use_.compare_exchange_strong(expected, true,
                                                           std::memory_order_acquire);
                }
                return false;
            }

            // next_entry is only set once when it gets inserted into the list and
            // is never changed afterwards
            // -> therefore it does not have to be atomic
            T *next_entry_;

            // in_use_ is only used to manage ownership of entries
            // -> therefore all operations on it can use relaxed order
            std::atomic<bool> in_use_;
        };

        class iterator : public std::iterator<std::forward_iterator_tag, T> {
            T *ptr = nullptr;

            explicit iterator(T *ptr) : ptr(ptr) {}

           public:
            iterator() = default;

            void swap(iterator &other) noexcept { std::swap(ptr, other.ptr); }

            iterator &operator++() {
                assert(ptr != nullptr);
                ptr = ptr->next_entry_;
                return *this;
            }

            iterator operator++(int) {
                assert(ptr != nullptr);
                iterator tmp(*this);
                ptr = ptr->next_entry_;
                return tmp;
            }

            bool operator==(const iterator &rhs) const { return ptr == rhs.ptr; }

            bool operator!=(const iterator &rhs) const { return ptr != rhs.ptr; }

            T &operator*() const {
                assert(ptr != nullptr);
                return *ptr;
            }

            T *operator->() const {
                assert(ptr != nullptr);
                return ptr;
            }

            friend class ThreadBlockList;
        };

        ThreadControlBlock *acquire_entry() { return adopt_or_create_entry(); }

        void release_entry(T *entry) { entry->abandon(); }

        iterator begin() { return iterator{head_.load(std::memory_order_acquire)}; }

        iterator end() { return iterator{}; }

        void abandon_retired_nodes(Orphan<kNumberEpochs> *obj) {
            auto last = obj;
            auto next = last->next_;
            while (next) {
                last = next;
                next = last->next_;
            }

            auto h = abandoned_retired_nodes_.load(std::memory_order_relaxed);
            do {
                last->next_ = h;
            } while (!abandoned_retired_nodes_.compare_exchange_weak(
                h, obj, std::memory_order_release, std::memory_order_relaxed));
        }

        Orphan<kNumberEpochs> *adopt_abandoned_retired_nodes() {
            if (abandoned_retired_nodes_.load(std::memory_order_relaxed) == nullptr) return nullptr;
            return abandoned_retired_nodes_.exchange(nullptr, std::memory_order_acquire);
        }

       private:
        void add_entry(T *node) {
            auto h = head_.load(std::memory_order_relaxed);
            do {
                node->next_entry_ = h;
            } while (!head_.compare_exchange_weak(h, node, std::memory_order_release,
                                                  std::memory_order_relaxed));
        }

        T *adopt_or_create_entry() {
            T *result = head_.load(std::memory_order_acquire);
            while (result) {
                if (result->tryAdopt()) return result;

                result = result->next_entry_;
            }

            result = new T();
            add_entry(result);
            return result;
        }

        std::atomic<T *> head_;

        // collected garbage when a thread exit
        alignas(64) std::atomic<Orphan<kNumberEpochs> *> abandoned_retired_nodes_;
    };

    /**
     * get the local ThreadData
     */
    static ThreadData &getLocalThreadData() {
        static thread_local ThreadData local_thread_data;
        return local_thread_data;
    }

    static std::atomic<unsigned> global_epoch_;

    static ThreadBlockList<ThreadControlBlock> global_thread_block_list_;
};

template <std::size_t UpdateThreshold, class Deallocator>
std::atomic<unsigned> EBR<UpdateThreshold, Deallocator>::global_epoch_;

template <std::size_t UpdateThreshold, class Deallocator>
typename EBR<UpdateThreshold, Deallocator>::template ThreadBlockList<typename EBR<UpdateThreshold, Deallocator>::ThreadControlBlock> 
    EBR<UpdateThreshold, Deallocator>::global_thread_block_list_;

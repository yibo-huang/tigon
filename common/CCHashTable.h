#pragma once

#include "stdint.h"
#include "common/CXLMemory.h"
#include "common/CCSet.h"

#include "atomic_offset_ptr.hpp"
#include <boost/interprocess/offset_ptr.hpp>

namespace star
{

class CCHashTable {
    public:
	void init(uint32_t origin_index_id, uint64_t buckets_cnt)
        {
                int i = 0;

                this->origin_index_id = origin_index_id;
                this->bucket_cnt = bucket_cnt;

                buckets = (CCBucket *)CXLMemory::cxlalloc_malloc_wrapper(sizeof(CCBucket) * bucket_cnt);
                for (i = 0; i < bucket_cnt; i++)
                        buckets[i].init();
        }

	CCSet *read(uint64_t key)
        {
                CCBucket *cur_bkt = &buckets[hash(key)];
	        return cur_bkt->read(key);
        }

	bool insert(uint64_t key, char *row)
        {
                uint64_t bkt_idx = hash(key);
                CCBucket *cur_bkt = &buckets[bkt_idx];

                bool success = cur_bkt->insert(key, row);
                if (success == true) {
                        return true;
                } else {
                        DCHECK(0);
                        return false;
                }
        }

	bool remove(char *row)
        {
                /* not implemented and never used */
                assert(0);
                return false;
        }

    private:
	class CCNode {
	    public:
		CCNode(uint64_t key)
                {
                        this->key = key;
                        next = NULL;
                        rows.clear();
                }

		uint64_t key;
                CCSet rows;
                boost::interprocess::offset_ptr<CCNode> next;
	};

	class CCBucket {
	    public:
		void init()
                {
                        first_node.store(NULL, std::memory_order_release);
                        pthread_spin_init(&latch, PTHREAD_PROCESS_SHARED);
                }

		CCSet *read(uint64_t key)
                {
                        pthread_spin_lock(&latch);
                        CCNode *node = find_node(key);
                        pthread_spin_unlock(&latch);

                        if (node != NULL)
                                return &node->rows;
                        else
                                return NULL;
                }

		bool insert(uint64_t key, char *row)
                {
                        CCNode *node = NULL;

                        pthread_spin_lock(&latch);
                        node = find_node(key);
                        if (node == NULL) {
                                node = (CCNode *)CXLMemory::cxlalloc_malloc_wrapper(sizeof(CCNode));
                                new(node) CCNode(key);
                                node->rows.insert(row);
                                node->next = first_node.load(std::memory_order_acquire);
                                first_node.store(node, std::memory_order_release);      // linearization point
                        } else {
                                // TODO. should diferentiate between unique vs. nonunique indexes.
                                node->rows.insert(row);
                        }
                        pthread_spin_unlock(&latch);
                        return true;
                }

		void remove(uint64_t index_key, char *row)
                {
                        /* not implemented and never used */
                        assert(0);
                }

	    private:
                CCNode *find_node(uint64_t key)
                {
                        CCNode *cur_node = first_node.load(std::memory_order_acquire);
                        while (cur_node != NULL) {
                                if (cur_node->key == key)
                                        return cur_node;
                                cur_node = cur_node->next.get();
                        }
                        return NULL;
                }

		AtomicOffsetPtr<CCNode> first_node;
		pthread_spinlock_t latch;
	};

	uint64_t hash(uint64_t key)
        {
                return (key ^ (key / bucket_cnt)) % bucket_cnt;
        }

	boost::interprocess::offset_ptr<CCBucket> buckets;
	uint64_t bucket_cnt;

        uint32_t origin_index_id;
};

} // namespace star

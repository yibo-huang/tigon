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
	CCHashTable(uint64_t bucket_cnt)
                : bucket_cnt(bucket_cnt)
        {
                buckets = reinterpret_cast<CCBucket *>(CXLMemory::cxlalloc_malloc_wrapper(sizeof(CCBucket) * bucket_cnt));
                for (int i = 0; i < bucket_cnt; i++)
                        new(&buckets[i]) CCBucket();
        }

	CCSet *search(uint64_t key)
        {
                CCBucket *cur_bkt = &buckets[hash(key)];
	        return cur_bkt->search(key);
        }

	bool insert(uint64_t key, char *row)
        {
                CCBucket *cur_bkt = &buckets[hash(key)];
                return cur_bkt->insert(key, row);
        }

	bool remove(char *row)
        {
                /* not implemented and never used */
                DCHECK(0);
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
                CCBucket()
                {
                        first_node = nullptr;
                        pthread_spin_init(&latch, PTHREAD_PROCESS_SHARED);
                }

		CCSet *search(uint64_t key)
                {
                        CCNode *node = nullptr;

                        pthread_spin_lock(&latch);
                        node = find_node(key);
                        pthread_spin_unlock(&latch);

                        if (node != nullptr)
                                return &node->rows;
                        else
                                return nullptr;
                }

		bool insert(uint64_t key, char *row)
                {
                        CCNode *node = nullptr;

                        pthread_spin_lock(&latch);
                        node = find_node(key);
                        if (node == NULL) {
                                node = reinterpret_cast<CCNode *>(CXLMemory::cxlalloc_malloc_wrapper(sizeof(CCNode)));
                                new(node) CCNode(key);
                                node->rows.insert(row);
                                node->next = first_node.get();
                                first_node = node;
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
                        DCHECK(0);
                }

	    private:
                CCNode *find_node(uint64_t key)
                {
                        CCNode *cur_node = first_node.get();

                        while (cur_node != NULL) {
                                if (cur_node->key == key) {
                                        return cur_node;
                                }
                                cur_node = cur_node->next.get();
                        }
                        return NULL;
                }

		boost::interprocess::offset_ptr<CCNode> first_node;
		pthread_spinlock_t latch;
	};

	uint64_t hash(uint64_t key)
        {
                return (key ^ (key / bucket_cnt)) % bucket_cnt;
        }

	boost::interprocess::offset_ptr<CCBucket> buckets;
	uint64_t bucket_cnt;
};

} // namespace star

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

	char *search(uint64_t key)
        {
                CCBucket *cur_bkt = &buckets[hash(key)];
	        return cur_bkt->search(key);
        }

	bool insert(uint64_t key, char *row)
        {
                CCBucket *cur_bkt = &buckets[hash(key)];
                return cur_bkt->insert(key, row);
        }

	bool remove(uint64_t key, char *row)
        {
                CCBucket *cur_bkt = &buckets[hash(key)];
                return cur_bkt->remove(key, row);
        }

    private:
	class CCNode {
	    public:
		CCNode(uint64_t key)
                {
                        this->key = key;
                        next = nullptr;
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

		char *search(uint64_t key)
                {
                        CCNode *node = nullptr;

                        pthread_spin_lock(&latch);
                        node = find_node(key);
                        pthread_spin_unlock(&latch);

                        // note that we never delete the node
                        // so we need to check if it is empty
                        if (node != nullptr && node->rows.empty() == false) {
                                CHECK(node->rows.size() == 1);
                                return node->rows.get_element(0);
                        } else {
                                return nullptr;
                        }
                }

		bool insert(uint64_t key, char *row)
                {
                        CCNode *node = nullptr;
                        bool ret = false;

                        pthread_spin_lock(&latch);
                        node = find_node(key);
                        if (node == nullptr) {
                                node = reinterpret_cast<CCNode *>(CXLMemory::cxlalloc_malloc_wrapper(sizeof(CCNode)));
                                new(node) CCNode(key);
                                ret = node->rows.insert(row);   // insert row into node
                                DCHECK(ret == true);
                                node->next = first_node.get();  // insert node into bucket head
                                first_node = node;
                        } else {
                                // TODO. should differentiate between unique vs. nonunique indexes.
                                ret = node->rows.insert(row);   // insert can fail if the row already exists
                        }
                        pthread_spin_unlock(&latch);
                        return ret;
                }

		bool remove(uint64_t key, char *row)
                {
                        CCNode *node = nullptr;
                        bool ret = false;

                        pthread_spin_lock(&latch);
                        node = find_node(key);
                        if (node != nullptr)
                                ret = node->rows.remove(row);   // remove row from node
                        // note that we do not delete the node
                        // this avoids frequent alloc & free
                        pthread_spin_unlock(&latch);
                        return ret;
                }

	    private:
                CCNode *find_node(uint64_t key)
                {
                        CCNode *cur_node = first_node.get();

                        while (cur_node != nullptr) {
                                if (cur_node->key == key) {
                                        return cur_node;
                                }
                                cur_node = cur_node->next.get();
                        }
                        return nullptr;
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

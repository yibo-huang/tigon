//
// Created by Yi Lu on 7/18/18.
//

#pragma once

#include "common/CCHashTable.h"
#include "common/btree_olc/BTreeOLC.h"

namespace star
{

class CXLTableBase {
    public:
	virtual ~CXLTableBase() = default;

	virtual void *search(const void *key) = 0;

        virtual void scan(const void *min_key, const void *max_key, uint64_t limit, void *results) = 0;

	virtual bool insert(const void *key, void *row, bool is_placeholder = false) = 0;

        virtual void make_placeholder_valid(const void *key) = 0;

        virtual bool remove(const void *key, void *row) = 0;

	virtual std::size_t tableID() = 0;

	virtual std::size_t partitionID() = 0;
};

template <class KeyType> class CXLTableHashMap : public CXLTableBase {
    public:
	virtual ~CXLTableHashMap() override = default;

        CXLTableHashMap(CCHashTable *cxl_hashtable, std::size_t tableID, std::size_t partitionID)
		: cxl_hashtable(cxl_hashtable)
                , tableID_(tableID)
		, partitionID_(partitionID)
	{
	}

	virtual void *search(const void *key) override
        {
                const auto &k = *static_cast<const KeyType *>(key);
                return cxl_hashtable->search(k);
        }

        virtual void scan(const void *min_key, const void *max_key, uint64_t limit, void *results) override
        {
                CHECK(0);
        }

	virtual bool insert(const void *key, void *row, bool is_placeholder = false) override
        {
                CHECK(is_placeholder == false);
                const auto &k = *static_cast<const KeyType *>(key);
                return cxl_hashtable->insert(k, reinterpret_cast<char *>(row));
        }

        virtual void make_placeholder_valid(const void *key) override
        {
                CHECK(0);
        }

        virtual bool remove(const void *key, void *row) override
        {
                const auto &k = *static_cast<const KeyType *>(key);
                return cxl_hashtable->remove(k, reinterpret_cast<char *>(row));
        }

	virtual std::size_t tableID() override
        {
                return tableID_;
        }

	virtual std::size_t partitionID() override
        {
                return partitionID_;
        }

    private:
	CCHashTable *cxl_hashtable;
	std::size_t tableID_;
	std::size_t partitionID_;
};

template <class KeyType, class KeyComparator> class CXLTableBTreeOLC : public CXLTableBase {
    public:
	virtual ~CXLTableBTreeOLC() override = default;

        CXLTableBTreeOLC(std::size_t tableID, std::size_t partitionID)
                : tableID_(tableID)
		, partitionID_(partitionID)
	{
                CHECK(0);
	}

	virtual void *search(const void *key) override
        {
                CHECK(0);
        }

        virtual void scan(const void *min_key, const void *max_key, uint64_t limit, void *results) override
        {
                CHECK(0);
        }

	virtual bool insert(const void *key, void *row, bool is_placeholder = false) override
        {
                CHECK(0);
        }

        virtual void make_placeholder_valid(const void *key) override
        {
                CHECK(0);
        }

        virtual bool remove(const void *key, void *row) override
        {
                CHECK(0);
        }

	virtual std::size_t tableID() override
        {
                return tableID_;
        }

	virtual std::size_t partitionID() override
        {
                return partitionID_;
        }

    private:
	CCHashTable *cxl_hashtable;
        uint64_t bucket_cnt;
	std::size_t tableID_;
	std::size_t partitionID_;
};

} // namespace star

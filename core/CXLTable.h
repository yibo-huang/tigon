//
// Created by Yi Lu on 7/18/18.
//

#pragma once

#include "common/CCHashTable.h"
#include "common/btree_olc_cxl/BTreeOLC_CXL.h"

#include <boost/interprocess/offset_ptr.hpp>

namespace star
{

class CXLTableBase {
    public:
	virtual ~CXLTableBase() = default;

	virtual void *search(const void *key) = 0;

        virtual void scan(const void *min_key, const void *max_key, uint64_t limit, void *results_ptr) = 0;

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
		: cxl_hashtable_(cxl_hashtable)
                , tableID_(tableID)
		, partitionID_(partitionID)
	{
	}

	virtual void *search(const void *key) override
        {
                const auto &k = *static_cast<const KeyType *>(key);
                return cxl_hashtable_->search(k.get_plain_key());
        }

        virtual void scan(const void *min_key, const void *max_key, uint64_t limit, void *results_ptr) override
        {
                CHECK(0);
        }

	virtual bool insert(const void *key, void *row, bool is_placeholder = false) override
        {
                CHECK(is_placeholder == false);
                const auto &k = *static_cast<const KeyType *>(key);
                return cxl_hashtable_->insert(k.get_plain_key(), reinterpret_cast<char *>(row));
        }

        virtual void make_placeholder_valid(const void *key) override
        {
                CHECK(0);
        }

        virtual bool remove(const void *key, void *row) override
        {
                const auto &k = *static_cast<const KeyType *>(key);
                return cxl_hashtable_->remove(k.get_plain_key(), reinterpret_cast<char *>(row));
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
	CCHashTable *cxl_hashtable_;
	std::size_t tableID_;
	std::size_t partitionID_;
};

template <class KeyType, class KeyComparator> class CXLTableBTreeOLC : public CXLTableBase {
    public:
        static constexpr uint64_t update_threshold = 1024;
        static constexpr uint64_t leaf_page_size = 4096;
        static constexpr uint64_t inner_page_size = 4096;

        // std::atomic has implicitly deleted copy-constructor
        // so we need to define a ValueType that supports it
        struct BTreeOLCValue {
                BTreeOLCValue() = default;

                BTreeOLCValue(const BTreeOLCValue &value)
                {
                        this->row = value.row.get();
                        this->is_valid.store(value.is_valid.load());
                }

                BTreeOLCValue &operator=(const BTreeOLCValue &value)
                {
                        this->row = value.row.get();
                        this->is_valid.store(value.is_valid.load());
                        return *this;
                }

                boost::interprocess::offset_ptr<void> row{ nullptr };
                std::atomic<bool> is_valid{ false };
        };

        struct BTreeOLCValueComparator {
                int operator()(const BTreeOLCValue &a, const BTreeOLCValue &b) const
                {
                        if (a.row.get() == b.row.get())
                                return 0;
                        else
                                return 1;
                }
        };

        using CXLBTree = btreeolc_cxl::BPlusTree<KeyType, BTreeOLCValue, KeyComparator, BTreeOLCValueComparator, update_threshold, leaf_page_size, inner_page_size>;

	virtual ~CXLTableBTreeOLC() override = default;

        CXLTableBTreeOLC(CXLBTree *cxl_btree, std::size_t tableID, std::size_t partitionID)
                : cxl_btree_(cxl_btree)
                , tableID_(tableID)
		, partitionID_(partitionID)
	{
	}

	virtual void *search(const void *key) override
        {
                const auto &k = *static_cast<const KeyType *>(key);

                BTreeOLCValue *value_ptr = nullptr;
                bool success = cxl_btree_->lookup(k, value_ptr);

                if (value_ptr != nullptr) {
                        if (value_ptr->is_valid.load() == true) {
                                return value_ptr->row.get();
                        } else {
                                return nullptr;
                        }
                } else {
                        return nullptr;
                }
        }

        virtual void scan(const void *min_key, const void *max_key, uint64_t limit, void *results_ptr) override
        {
                const auto &min_k = *static_cast<const KeyType *>(min_key);
                const auto &max_k = *static_cast<const KeyType *>(max_key);
                auto &results = *static_cast<std::vector<std::tuple<KeyType, void *> > *>(results_ptr);

                auto processor = [&](const KeyType &key, BTreeOLCValue &value, bool) -> bool {
                        if (limit != 0 && results.size() == limit)
                                return true;

                        if (KeyComparator()(key, max_k) > 0)
                                return true;

                        if (value.is_valid.load() == true) {
                                CHECK(KeyComparator()(key, min_k) >= 0);
                                std::tuple<KeyType, void *> row_tuple(key, value.row.get());
                                results.push_back(row_tuple);
                        }

                        return false;
		};

                cxl_btree_->scanForUpdate(min_k, processor);
        }

	virtual bool insert(const void *key, void *row, bool is_placeholder = false) override
        {
                const auto &k = *static_cast<const KeyType *>(key);

                BTreeOLCValue value;
                value.row = row;
                if (is_placeholder == true)
                        value.is_valid.store(false);
                else
                        value.is_valid.store(true);

		bool success = cxl_btree_->insert(k, value);
		return success;
        }

        virtual void make_placeholder_valid(const void *key) override
        {
                const auto &k = *static_cast<const KeyType *>(key);

                BTreeOLCValue *value_ptr;
                bool success = cxl_btree_->lookup(k, value_ptr);
                CHECK(success == true);
                CHECK(value_ptr->is_valid.load() == false);
                value_ptr->is_valid.store(true);
        }

        virtual bool remove(const void *key, void *row) override
        {
                const auto &k = *static_cast<const KeyType *>(key);

                bool success = cxl_btree_->remove(k);
                CHECK(success == true);

                return success;
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
	CXLBTree *cxl_btree_;
	std::size_t tableID_;
	std::size_t partitionID_;
};

} // namespace star

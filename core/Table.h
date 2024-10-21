//
// Created by Yi Lu on 7/18/18.
//

#pragma once

#include <thread>
#include "benchmark/tpcc/Schema.h"
#include "common/ClassOf.h"
#include "common/Encoder.h"
#include "common/HashMap.h"
#include "common/StringPiece.h"
#include "common/btree_olc/BTreeOLC.h"

static thread_local uint64_t tid = std::numeric_limits<uint64_t>::max();
extern bool do_tid_check;
static std::hash<std::thread::id> tid_hasher;
namespace star
{

extern void tid_check();

class ITable {
    public:
        enum { HASHMAP, BTREE };

	using MetaDataType = std::atomic<uint64_t>;

	virtual ~ITable() = default;

        virtual uint64_t get_plain_key(const void *key) = 0;

        virtual int compare_key(const void *a, const void *b) = 0;

	virtual std::tuple<MetaDataType *, void *> search(const void *key) = 0;

	virtual bool contains(const void *key)
	{
		return true;
	}

	virtual void *search_value(const void *key) = 0;

	virtual MetaDataType *search_metadata(const void *key) = 0;

        virtual void scan(const void *min_key, const void *max_key, uint64_t limit, void *results_ptr) = 0;

	virtual bool insert(const void *key, const void *value, bool is_placeholder = false) = 0;

        virtual bool remove(const void *key) = 0;

	virtual void update(
		const void *key, const void *value, std::function<void(const void *, const void *)> on_update = [](const void *, const void *) {}) = 0;

        virtual bool search_and_update_next_key_info(const void *key,
                std::function<void(const void *prev_key, void *prev_value, const void *cur_key, void *cur_value, const void *next_key, void *next_value)> update_processor) = 0;

	virtual void deserialize_value(const void *key, StringPiece stringPiece) = 0;

	virtual void serialize_value(Encoder &enc, const void *value) = 0;

	virtual std::size_t key_size() = 0;

	virtual std::size_t value_size() = 0;

	virtual std::size_t field_size() = 0;

	virtual std::size_t tableID() = 0;

	virtual std::size_t partitionID() = 0;

        virtual int tableType() = 0;

	virtual void turn_on_cow()
	{
	}

	virtual void dump_copy(std::function<void(const void *, const void *)> dump_processor, std::function<void()> unlock_processor)
	{
	}

	virtual bool cow_dump_finished()
	{
		return true;
	}

	virtual std::function<void()> turn_off_cow()
	{
		return []() {};
	}

        virtual void move_all_into_cxl(std::function<bool(ITable *, const void *, std::tuple<MetaDataType *, void *> &)> move_in_func)
        {
                CHECK(0);
        }
};

class MetaInitFuncNothing {
    public:
	uint64_t operator()(bool is_tuple_valid = true)
	{
		return 0;
	}
};

extern uint64_t SundialMetadataInit(bool is_tuple_valid);
class MetaInitFuncSundial {
    public:
	uint64_t operator()(bool is_tuple_valid = true)
	{
		return SundialMetadataInit(is_tuple_valid);
	}
};

extern uint64_t SundialPashaMetadataLocalInit(bool is_tuple_valid);
class MetaInitFuncSundialPasha {
    public:
	uint64_t operator()(bool is_tuple_valid = true)
	{
		return SundialPashaMetadataLocalInit(is_tuple_valid);
	}
};

extern uint64_t TwoPLMetadataInit(bool is_tuple_valid);
class MetaInitFuncTwoPL {
    public:
	uint64_t operator()(bool is_tuple_valid = true)
	{
		return TwoPLMetadataInit(is_tuple_valid);
	}
};

extern uint64_t TwoPLPashaMetadataLocalInit(bool is_tuple_valid);
class MetaInitFuncTwoPLPasha {
    public:
	uint64_t operator()(bool is_tuple_valid = true)
	{
		return TwoPLPashaMetadataLocalInit(is_tuple_valid);
	}
};

template <std::size_t N, class KeyType, class ValueType, class KeyComparator, class ValueComparator, class MetaInitFunc = MetaInitFuncNothing> class TableHashMap : public ITable {
    public:
	using MetaDataType = std::atomic<uint64_t>;

	virtual ~TableHashMap() override = default;

	TableHashMap(std::size_t tableID, std::size_t partitionID)
		: tableID_(tableID)
		, partitionID_(partitionID)
	{
	}

        uint64_t get_plain_key(const void *key) override
        {
                tid_check();
                const auto &k = *static_cast<const KeyType *>(key);
                return k.get_plain_key();
        }

        int compare_key(const void *a, const void *b) override
        {
                const auto &k_a = *static_cast<const KeyType *>(a);
                const auto &k_b = *static_cast<const KeyType *>(b);
                return KeyComparator()(k_a, k_b);
        }

	std::tuple<MetaDataType *, void *> search(const void *key) override
	{
                CHECK(contains(key) == true);
		tid_check();
		const auto &k = *static_cast<const KeyType *>(key);
                CHECK(map_.contains(k) == true);
		auto &v = map_[k];
		return std::make_tuple(&std::get<0>(v), &std::get<1>(v));
	}

	void *search_value(const void *key) override
	{
                CHECK(contains(key) == true);
		tid_check();
		const auto &k = *static_cast<const KeyType *>(key);
		return &std::get<1>(map_[k]);
	}

	MetaDataType *search_metadata(const void *key) override
	{
                CHECK(contains(key) == true);
		tid_check();
		const auto &k = *static_cast<const KeyType *>(key);
		return &std::get<0>(map_[k]);
	}

	bool contains(const void *key) override
	{
		const auto &k = *static_cast<const KeyType *>(key);
		return map_.contains(k);
	}

        void scan(const void *min_key, const void *max_key, uint64_t limit, void *results_ptr) override
        {
                // hash table does not support scan
                CHECK(0);
        }

        bool insert(const void *key, const void *value, bool is_placeholder = false) override
	{
                // hash table does not handle concurrent inserts, so it will always succeed
		tid_check();
		const auto &k = *static_cast<const KeyType *>(key);
		const auto &v = *static_cast<const ValueType *>(value);
		bool ok = map_.contains(k);
		DCHECK(ok == false);
		auto &row = map_[k];
		std::get<0>(row).store(MetaInitFunc()());
		std::get<1>(row) = v;

                return true;
	}

        bool remove(const void *key) override
        {
                CHECK(0);
        }

	void update(const void *key, const void *value, std::function<void(const void *, const void *)> on_update) override
	{
		tid_check();
		const auto &k = *static_cast<const KeyType *>(key);
		const auto &v = *static_cast<const ValueType *>(value);
		auto &row = map_[k];
		on_update(key, &std::get<1>(row));
		std::get<1>(row) = v;
	}

        bool search_and_update_next_key_info(const void *key,
                std::function<void(const void *prev_key, void *prev_value, const void *cur_key, void *cur_value, const void *next_key, void *next_value)> update_processor) override
        {
                // no need to maintain next-key information for unordered tables
                return true;
        }

	void deserialize_value(const void *key, StringPiece stringPiece) override
	{
		tid_check();
		std::size_t size = stringPiece.size();
		const auto &k = *static_cast<const KeyType *>(key);
		auto &row = map_[k];
		auto &v = std::get<1>(row);

		Decoder dec(stringPiece);
		dec >> v;

		DCHECK(size - dec.size() == ClassOf<ValueType>::size());
	}

	void serialize_value(Encoder &enc, const void *value) override
	{
		tid_check();
		std::size_t size = enc.size();
		const auto &v = *static_cast<const ValueType *>(value);
		enc << v;

		DCHECK(enc.size() - size == ClassOf<ValueType>::size());
	}

	std::size_t key_size() override
	{
		tid_check();
		return sizeof(KeyType);
	}

	std::size_t value_size() override
	{
		tid_check();
		return sizeof(ValueType);
	}

	std::size_t field_size() override
	{
		tid_check();
		return ClassOf<ValueType>::size();
	}

	std::size_t tableID() override
	{
		tid_check();
		return tableID_;
	}

	std::size_t partitionID() override
	{
		tid_check();
		return partitionID_;
	}

        int tableType() override
        {
                return HASHMAP;
        }

        void move_all_into_cxl(std::function<bool(ITable *, const void *, std::tuple<MetaDataType *, void *> &)> move_in_func) override
        {
                auto processor = [&](const KeyType &key, std::tuple<MetaDataType, ValueType> &row) {
                        MetaDataType *meta_ptr = &std::get<0>(row);
                        ValueType *data_ptr = &std::get<1>(row);
                        std::tuple<MetaDataType *, void *> row_tuple(meta_ptr, data_ptr);
			bool ret = move_in_func(this, &key, row_tuple);
                        CHECK(ret == true);
		};

                map_.iterate_non_const(processor, []() {});
        }

    private:
	HashMap<N, KeyType, std::tuple<MetaDataType, ValueType> > map_;
	std::size_t tableID_;
	std::size_t partitionID_;
};

template <class KeyType, class ValueType, class KeyComparator, class ValueComparator, class MetaInitFunc = MetaInitFuncNothing> class TableBTreeOLC : public ITable {
    public:
        using MetaDataType = std::atomic<uint64_t>;

        static constexpr uint64_t update_threshold = 1024;
        static constexpr uint64_t leaf_page_size = 4096;
        static constexpr uint64_t inner_page_size = 4096;

        struct ValueStruct {
                MetaDataType meta;     // the value is the pointer to the local metadata
                ValueType data;
        };

        // std::atomic has implicitly deleted copy-constructor
        // so we need to define a ValueType that supports it
        struct BTreeOLCValue {
                BTreeOLCValue() = default;

                BTreeOLCValue(const BTreeOLCValue &value)
                {
                        this->row = value.row;
                }

                BTreeOLCValue &operator=(const BTreeOLCValue &value)
                {
                        this->row = value.row;
                        return *this;
                }

                ValueStruct *row{ nullptr };
        };

        struct BTreeOLCValueComparator {
                int operator()(const BTreeOLCValue &a, const BTreeOLCValue &b) const
                {
                        if (a.row == b.row)
                                return 0;
                        else
                                return 1;
                }
        };

        using BTree = btreeolc::BPlusTree<KeyType, BTreeOLCValue, KeyComparator, BTreeOLCValueComparator, update_threshold, leaf_page_size, inner_page_size>;

	virtual ~TableBTreeOLC() override = default;

	TableBTreeOLC(std::size_t tableID, std::size_t partitionID)
		: tableID_(tableID)
		, partitionID_(partitionID)
	{
	}

        uint64_t get_plain_key(const void *key) override
        {
                tid_check();
                const auto &k = *static_cast<const KeyType *>(key);
                return k.get_plain_key();
        }

        int compare_key(const void *a, const void *b) override
        {
                const auto &k_a = *static_cast<const KeyType *>(a);
                const auto &k_b = *static_cast<const KeyType *>(b);
                return KeyComparator()(k_a, k_b);
        }

	std::tuple<MetaDataType *, void *> search(const void *key) override
	{
                tid_check();
		const auto &k = *static_cast<const KeyType *>(key);

                BTreeOLCValue value;
                bool success = btree.lookup(k, value);

                if (success == true) {
                        MetaDataType *meta_ptr = reinterpret_cast<MetaDataType *>(&value.row->meta);
                        return std::make_tuple(meta_ptr, &value.row->data);
                } else {
                        return std::make_tuple(nullptr, nullptr);
                }
	}

	void *search_value(const void *key) override
	{
                tid_check();
		const auto &k = *static_cast<const KeyType *>(key);

                BTreeOLCValue value;
                bool success = btree.lookup(k, value);

                if (success == true) {
                        return &value.row->data;
                } else {
                        return nullptr;
                }
	}

	MetaDataType *search_metadata(const void *key) override
	{
                tid_check();
		const auto &k = *static_cast<const KeyType *>(key);

                BTreeOLCValue value;
                bool success = btree.lookup(k, value);

                if (success == true) {
                        return &value.row->meta;
                } else {
                        return nullptr;
                }
	}

	bool contains(const void *key) override
	{
                tid_check();
		const auto &k = *static_cast<const KeyType *>(key);

                BTreeOLCValue value;
                bool success = btree.lookup(k, value);

                if (success) {
                        return true;
                } else {
                        return false;
                }
	}

        void scan(const void *min_key, const void *max_key, uint64_t limit, void *results_ptr) override
        {
                tid_check();
                const auto &min_k = *static_cast<const KeyType *>(min_key);
                const auto &max_k = *static_cast<const KeyType *>(max_key);
                auto &results = *static_cast<std::vector<std::tuple<KeyType, std::atomic<uint64_t> *, void *> > *>(results_ptr);

                auto processor = [&](const KeyType &key, BTreeOLCValue &value, bool) -> bool {
                        if (limit != 0 && results.size() == limit)
                                return true;

                        if (KeyComparator()(key, max_k) > 0)
                                return true;

                        CHECK(KeyComparator()(key, min_k) >= 0);
                        MetaDataType *meta_ptr = &value.row->meta;
                        ValueType *data_ptr = &value.row->data;
                        std::tuple<KeyType, MetaDataType *, void *> row_tuple(key, meta_ptr, data_ptr);
                        results.push_back(row_tuple);

                        return false;
		};

                btree.scanForUpdate(min_k, processor);
        }

	bool insert(const void *key, const void *value, bool is_placeholder = false) override
	{
                tid_check();
		const auto &k = *static_cast<const KeyType *>(key);
		const auto &v = *static_cast<const ValueType *>(value);

                bool is_tuple_valid = !is_placeholder;

                // create value that will not be moved around
                ValueStruct *row = new ValueStruct;
                CHECK(row != nullptr);
                row->meta = MetaInitFunc()(is_tuple_valid);
                row->data = v;

                // BTreeOLCValue will be moved around and thus only stores pointers to the actual value
                BTreeOLCValue btree_value;
                btree_value.row = row;

                // insert BTreeOLCValue to BTreeOLC
		bool success = btree.insert(k, btree_value);
		return success;
	}

        bool remove(const void *key) override
        {
                tid_check();
		const auto &k = *static_cast<const KeyType *>(key);

                bool success = btree.remove(k);
                CHECK(success == true);

                return success;
        }

	void update(const void *key, const void *value, std::function<void(const void *, const void *)> on_update) override
	{
                tid_check();
		const auto &k = *static_cast<const KeyType *>(key);
		const auto &v = *static_cast<const ValueType *>(value);

                BTreeOLCValue btree_value;
                bool success = btree.lookup(k, btree_value);
                CHECK(success == true);

		on_update(key, &btree_value.row->data);
		btree_value.row->data = v;
	}

        bool search_and_update_next_key_info(const void *key,
                std::function<void(const void *prev_key, void *prev_value, const void *cur_key, void *cur_value, const void *next_key, void *next_value)> update_processor) override
        {
                auto processor = [&](const KeyType *prev_key, BTreeOLCValue *prev_value, const KeyType *cur_key, BTreeOLCValue *cur_value, const KeyType *next_key, BTreeOLCValue *next_value) {
                        void *prev_meta = nullptr, *cur_meta = nullptr, *next_meta = nullptr;
                        if (prev_value != nullptr)
                                prev_meta = reinterpret_cast<void *>(prev_value->row->meta.load());
                        if (cur_value != nullptr)
                                cur_meta = reinterpret_cast<void *>(cur_value->row->meta.load());
                        if (next_value != nullptr)
                                next_meta = reinterpret_cast<void *>(next_value->row->meta.load());
                        update_processor(prev_key, prev_meta, cur_key, cur_meta, next_key, next_meta);
		};

                const auto &k = *static_cast<const KeyType *>(key);
                bool success = btree.lookupForNextKeyUpdate(k, processor);
                CHECK(success == true);

                return success;
        }

	void deserialize_value(const void *key, StringPiece stringPiece) override
	{
		tid_check();
		std::size_t size = stringPiece.size();
		const auto &k = *static_cast<const KeyType *>(key);

		BTreeOLCValue value;
                bool success = btree.lookup(k, value);
                CHECK(success == true);

		auto &v = value.row->data;

		Decoder dec(stringPiece);
		dec >> v;

		DCHECK(size - dec.size() == ClassOf<ValueType>::size());
	}


	void serialize_value(Encoder &enc, const void *value) override
	{
		tid_check();
		std::size_t size = enc.size();
		const auto &v = *static_cast<const ValueType *>(value);
		enc << v;

		DCHECK(enc.size() - size == ClassOf<ValueType>::size());
	}

	std::size_t key_size() override
	{
		tid_check();
		return sizeof(KeyType);
	}

	std::size_t value_size() override
	{
		tid_check();
		return sizeof(ValueType);
	}

	std::size_t field_size() override
	{
		tid_check();
		return ClassOf<ValueType>::size();
	}

	std::size_t tableID() override
	{
		tid_check();
		return tableID_;
	}

	std::size_t partitionID() override
	{
		tid_check();
		return partitionID_;
	}

        int tableType() override
        {
                return BTREE;
        }

        void move_all_into_cxl(std::function<bool(ITable *, const void *, std::tuple<MetaDataType *, void *> &)> move_in_func) override
        {
                auto processor = [&](const KeyType &key, BTreeOLCValue &value, bool) -> bool {
                        MetaDataType *meta_ptr = &value.row->meta;
                        ValueType *data_ptr = &value.row->data;
                        std::tuple<MetaDataType *, void *> row_tuple(meta_ptr, data_ptr);
			bool ret = move_in_func(this, &key, row_tuple);
                        CHECK(ret == true);
                        return false;
		};

                KeyType start_key;
                memset(&start_key, 0, sizeof(KeyType));
                CHECK(start_key.get_plain_key() == 0);
                btree.scanForUpdateNoContention(start_key, processor);
        }

    private:
	BTree btree;
	std::size_t tableID_;
	std::size_t partitionID_;
};

template <class KeyType, class ValueType, class KeyComparator, class ValueComparator> class HStoreTable : public ITable {
    public:
	using MetaDataType = std::atomic<uint64_t>;

	virtual ~HStoreTable() override = default;

	HStoreTable(std::size_t tableID, std::size_t partitionID)
		: tableID_(tableID)
		, partitionID_(partitionID)
	{
	}

        uint64_t get_plain_key(const void *key) override
        {
                tid_check();
                const auto &k = *static_cast<const KeyType *>(key);
                return k.get_plain_key();
        }

        int compare_key(const void *a, const void *b) override
        {
                const auto &k_a = *static_cast<const KeyType *>(a);
                const auto &k_b = *static_cast<const KeyType *>(b);
                return KeyComparator()(k_a, k_b);
        }

	std::tuple<MetaDataType *, void *> search(const void *key) override
	{
                CHECK(contains(key) == true);
		const auto &k = *static_cast<const KeyType *>(key);
		auto &v = map_[k];
		return std::make_tuple(nullptr, &(v));
	}

	void *search_value(const void *key) override
	{
                CHECK(contains(key) == true);
		const auto &k = *static_cast<const KeyType *>(key);
		return &map_[k];
	}

	MetaDataType *search_metadata(const void *key) override
	{
		static MetaDataType v;
		return &v;
	}

	bool contains(const void *key) override
	{
		const auto &k = *static_cast<const KeyType *>(key);
		return map_.contains(k);
	}

        void scan(const void *min_key, const void *max_key, uint64_t limit, void *results_ptr) override
        {
                // hash table does not support scan
                CHECK(0);
        }

	bool insert(const void *key, const void *value, bool is_placeholder = false) override
	{
		const auto &k = *static_cast<const KeyType *>(key);
		const auto &v = *static_cast<const ValueType *>(value);
		bool ok = map_.contains(k);
		DCHECK(ok == false);
		auto &row = map_[k];
		row = v;

                return true;
	}

        bool remove(const void *key) override
        {
                CHECK(0);
        }

	void update(const void *key, const void *value, std::function<void(const void *, const void *)> on_update) override
	{
		const auto &k = *static_cast<const KeyType *>(key);
		const auto &v = *static_cast<const ValueType *>(value);
		auto &row = map_[k];
		on_update(key, &row);
		row = v;
	}

        bool search_and_update_next_key_info(const void *key,
                std::function<void(const void *prev_key, void *prev_value, const void *cur_key, void *cur_value, const void *next_key, void *next_value)> update_processor) override
        {
                // no need to maintain next-key information for unordered tables
                return true;
        }

	void deserialize_value(const void *key, StringPiece stringPiece) override
	{
		std::size_t size = stringPiece.size();
		const auto &k = *static_cast<const KeyType *>(key);
		auto &row = map_[k];
		auto &v = row;

		Decoder dec(stringPiece);
		dec >> v;

		DCHECK(size - dec.size() == ClassOf<ValueType>::size());
	}

	void serialize_value(Encoder &enc, const void *value) override
	{
		std::size_t size = enc.size();
		const auto &v = *static_cast<const ValueType *>(value);
		enc << v;

		DCHECK(enc.size() - size == ClassOf<ValueType>::size());
	}

	std::size_t key_size() override
	{
		return sizeof(KeyType);
	}

	std::size_t value_size() override
	{
		return sizeof(ValueType);
	}

	std::size_t field_size() override
	{
		return ClassOf<ValueType>::size();
	}

	std::size_t tableID() override
	{
		return tableID_;
	}

	std::size_t partitionID() override
	{
		return partitionID_;
	}

        int tableType() override
        {
                return HASHMAP;
        }

    private:
	UnsafeHashMap<KeyType, ValueType> map_;
	std::size_t tableID_;
	std::size_t partitionID_;
};

template <std::size_t N, class KeyType, class ValueType, class KeyComparator, class ValueComparator> class HStoreCOWTable : public ITable {
    public:
	using MetaDataType = std::atomic<uint64_t>;

	virtual ~HStoreCOWTable() override = default;

	HStoreCOWTable(std::size_t tableID, std::size_t partitionID)
		: tableID_(tableID)
		, partitionID_(partitionID)
	{
	}

        uint64_t get_plain_key(const void *key) override
        {
                tid_check();
                const auto &k = *static_cast<const KeyType *>(key);
                return k.get_plain_key();
        }

        int compare_key(const void *a, const void *b) override
        {
                const auto &k_a = *static_cast<const KeyType *>(a);
                const auto &k_b = *static_cast<const KeyType *>(b);
                return KeyComparator()(k_a, k_b);
        }

	std::tuple<MetaDataType *, void *> search(const void *key) override
	{
                CHECK(contains(key) == true);
		const auto &k = *static_cast<const KeyType *>(key);
		auto &v = map_[k];
		return std::make_tuple(nullptr, &(v));
	}

	void *search_value(const void *key) override
	{
                CHECK(contains(key) == true);
		const auto &k = *static_cast<const KeyType *>(key);
		return &map_[k];
	}

	MetaDataType *search_metadata(const void *key) override
	{
		static MetaDataType v;
		return &v;
	}

	bool contains(const void *key) override
	{
		const auto &k = *static_cast<const KeyType *>(key);
		return map_.contains(k);
	}

        void scan(const void *min_key, const void *max_key, uint64_t limit, void *results_ptr) override
        {
                // hash table does not support scan
                CHECK(0);
        }

	bool insert(const void *key, const void *value, bool is_placeholder = false) override
	{
		const auto &k = *static_cast<const KeyType *>(key);
		const auto &v = *static_cast<const ValueType *>(value);
		bool ok = map_.contains(k);
		DCHECK(ok == false);
		auto &row = map_[k];
		std::get<1>(row) = v;

                return true;
	}

        bool remove(const void *key) override
        {
                CHECK(0);
        }

	void update(const void *key, const void *value, std::function<void(const void *, const void *)> on_update) override
	{
		const auto &k = *static_cast<const KeyType *>(key);
		const auto &v = *static_cast<const ValueType *>(value);
		auto &row = map_[k];
		if (dump_finished == false) {
			if (shadow_map_->contains(k) == false) { // Only store the first version
				auto &shadow_row = (*shadow_map_)[k];
				std::get<1>(shadow_row) = std::get<1>(row);
			}
		}
		on_update(key, &std::get<1>(row));
		std::get<1>(row) = v;
	}

        bool search_and_update_next_key_info(const void *key,
                std::function<void(const void *prev_key, void *prev_value, const void *cur_key, void *cur_value, const void *next_key, void *next_value)> update_processor) override
        {
                // no need to maintain next-key information for unordered tables
                return true;
        }

	void deserialize_value(const void *key, StringPiece stringPiece) override
	{
		std::size_t size = stringPiece.size();
		const auto &k = *static_cast<const KeyType *>(key);
		auto &row = map_[k];
		auto &v = std::get<1>(row);
		if (dump_finished == false) {
			if (shadow_map_->contains(k) == false) {
				auto &shadow_row = (*shadow_map_)[k];
				std::get<1>(shadow_row) = std::get<1>(row);
			}
		}
		Decoder dec(stringPiece);
		dec >> v;

		DCHECK(size - dec.size() == ClassOf<ValueType>::size());
	}

	void serialize_value(Encoder &enc, const void *value) override
	{
		std::size_t size = enc.size();
		const auto &v = *static_cast<const ValueType *>(value);
		enc << v;

		DCHECK(enc.size() - size == ClassOf<ValueType>::size());
	}

	std::size_t key_size() override
	{
		return sizeof(KeyType);
	}

	std::size_t value_size() override
	{
		return sizeof(ValueType);
	}

	std::size_t field_size() override
	{
		return ClassOf<ValueType>::size();
	}

	std::size_t tableID() override
	{
		return tableID_;
	}

	std::size_t partitionID() override
	{
		return partitionID_;
	}

        int tableType() override
        {
                return HASHMAP;
        }

	virtual void turn_on_cow() override
	{
		CHECK(cow == false);
		CHECK(shadow_map_ == nullptr);
		shadow_map_ = new HashMap<N, KeyType, std::tuple<MetaDataType, ValueType> >;
		dump_finished.store(false);
		cow.store(true);
	}

	virtual void dump_copy(std::function<void(const void *, const void *)> dump_processor, std::function<void()> dump_unlock) override
	{
		CHECK(cow == true);
		CHECK(dump_finished == false);
		CHECK(shadow_map_ != nullptr);
		auto processor = [&](const KeyType &key, const std::tuple<MetaDataType, ValueType> &row) {
			const auto &v = std::get<1>(row);
			dump_processor((const void *)&key, (const void *)&v);
		};
		map_.iterate(processor, dump_unlock);
		shadow_map_->iterate(processor, dump_unlock);
		dump_finished = true;
		auto clear_COW_status_bits_processor = [&](const KeyType &key, std::tuple<MetaDataType, ValueType> &row) {
			auto &meta = std::get<0>(row);
			meta.store(0);
		};
		map_.iterate_non_const(clear_COW_status_bits_processor, []() {});
	}

	virtual bool cow_dump_finished() override
	{
		return dump_finished;
	}

	virtual std::function<void()> turn_off_cow() override
	{
		CHECK(cow == true);
		auto shadow_map_ptr = shadow_map_;
		auto cleanup_work = [shadow_map_ptr]() { delete shadow_map_ptr; };
		shadow_map_ = nullptr;
		cow.store(false);
		return cleanup_work;
	}

    private:
	HashMap<N, KeyType, std::tuple<MetaDataType, ValueType> > map_;
	HashMap<N, KeyType, std::tuple<MetaDataType, ValueType> > *shadow_map_ = nullptr;
	std::size_t tableID_;
	std::size_t partitionID_;
	std::atomic<bool> dump_finished{ true };
	std::atomic<bool> cow{ false };
};
} // namespace star

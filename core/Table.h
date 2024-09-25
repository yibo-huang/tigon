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
	using MetaDataType = std::atomic<uint64_t>;

	virtual ~ITable() = default;

        virtual uint64_t get_plain_key(const void *key) = 0;

	virtual std::tuple<MetaDataType *, void *> search(const void *key) = 0;

	virtual bool contains(const void *key)
	{
		return true;
	}

	virtual void *search_value(const void *key) = 0;

	virtual MetaDataType &search_metadata(const void *key) = 0;

	virtual void insert(const void *key, const void *value) = 0;

	virtual void update(
		const void *key, const void *value, std::function<void(const void *, const void *)> on_update = [](const void *, const void *) {}) = 0;

	virtual void deserialize_value(const void *key, StringPiece stringPiece) = 0;

	virtual void serialize_value(Encoder &enc, const void *value) = 0;

	virtual std::size_t key_size() = 0;

	virtual std::size_t value_size() = 0;

	virtual std::size_t field_size() = 0;

	virtual std::size_t tableID() = 0;

	virtual std::size_t partitionID() = 0;

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

        virtual void move_all_into_cxl(std::function<bool(ITable *, uint64_t, std::tuple<MetaDataType *, void *> &)> move_in_func)
        {
        }
};

class MetaInitFuncNothing {
    public:
	uint64_t operator()()
	{
		return 0;
	}
};

extern uint64_t SundialMetadataInit();
class MetaInitFuncSundial {
    public:
	uint64_t operator()()
	{
		return SundialMetadataInit();
	}
};

extern uint64_t SundialPashaMetadataLocalInit();
class MetaInitFuncSundialPasha {
    public:
	uint64_t operator()()
	{
		return SundialPashaMetadataLocalInit();
	}
};

template <std::size_t N, class KeyType, class ValueType, class MetaInitFunc = MetaInitFuncNothing> class TableHashMap : public ITable {
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

	std::tuple<MetaDataType *, void *> search(const void *key) override
	{
		tid_check();
		const auto &k = *static_cast<const KeyType *>(key);
                CHECK(map_.contains(k) == true);
		auto &v = map_[k];
		return std::make_tuple(&std::get<0>(v), &std::get<1>(v));
	}

	void *search_value(const void *key) override
	{
		tid_check();
		const auto &k = *static_cast<const KeyType *>(key);
		return &std::get<1>(map_[k]);
	}

	MetaDataType &search_metadata(const void *key) override
	{
		tid_check();
		const auto &k = *static_cast<const KeyType *>(key);
		return std::get<0>(map_[k]);
	}

	bool contains(const void *key) override
	{
		const auto &k = *static_cast<const KeyType *>(key);
		return map_.contains(k);
	}

	void insert(const void *key, const void *value) override
	{
		tid_check();
		const auto &k = *static_cast<const KeyType *>(key);
		const auto &v = *static_cast<const ValueType *>(value);
		bool ok = map_.contains(k);
		DCHECK(ok == false);
		auto &row = map_[k];
		std::get<0>(row).store(MetaInitFunc()());
		std::get<1>(row) = v;
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

        void move_all_into_cxl(std::function<bool(ITable *, uint64_t, std::tuple<MetaDataType *, void *> &)> move_in_func) override
        {
                auto processor = [&](const KeyType &key, std::tuple<MetaDataType, ValueType> &row) {
                        MetaDataType *meta_ptr = &std::get<0>(row);
                        ValueType *data_ptr = &std::get<1>(row);
                        std::tuple<MetaDataType *, void *> row_tuple(meta_ptr, data_ptr);
			bool ret = move_in_func(this, get_plain_key(&key), row_tuple);
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

        struct TupleValueComparator {
                int operator()(const std::tuple<MetaDataType, ValueType> &a, const std::tuple<MetaDataType, ValueType> &b) const
                {
                        return ValueComparator(std::get<0>(a), std::get<0>(b));
                }
        };

        // std::atomic has implicitly deleted copy-constructor
        // so we need to define a ValueType that supports it
        struct BTreeOLCValue {
                BTreeOLCValue() = default;

                BTreeOLCValue(const BTreeOLCValue &row)
                {
                        this->meta.store(row.meta.load());
                        this->value = row.value;
                }

                BTreeOLCValue &operator=(const BTreeOLCValue &row)
                {
                        this->meta.store(row.meta.load());
                        this->value = row.value;
                        return *this;
                }

                MetaDataType meta;
                ValueType value;
        };

        using BTree = btreeolc::BPlusTree<KeyType, BTreeOLCValue, KeyComparator, TupleValueComparator, update_threshold, leaf_page_size, inner_page_size>;

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

	std::tuple<MetaDataType *, void *> search(const void *key) override
	{
                tid_check();
		const auto &k = *static_cast<const KeyType *>(key);

                BTreeOLCValue *row_ptr;
                bool success = btree.lookup(k, row_ptr);
                CHECK(success == true);

                MetaDataType *meta_ptr = reinterpret_cast<MetaDataType *>(&row_ptr->meta);
		return std::make_tuple(meta_ptr, &row_ptr->value);
	}

	void *search_value(const void *key) override
	{
                tid_check();
		const auto &k = *static_cast<const KeyType *>(key);

                BTreeOLCValue *row_ptr;
                bool success = btree.lookup(k, row_ptr);
                CHECK(success == true);

		return &row_ptr->value;
	}

	MetaDataType &search_metadata(const void *key) override
	{
                tid_check();
		const auto &k = *static_cast<const KeyType *>(key);
                BTreeOLCValue *row_ptr;
                bool success = btree.lookup(k, row_ptr);
                CHECK(success == true);

		return row_ptr->meta;
	}

	bool contains(const void *key) override
	{
                tid_check();
		const auto &k = *static_cast<const KeyType *>(key);

                BTreeOLCValue *row_ptr;
                return btree.lookup(k, row_ptr);
	}

	void insert(const void *key, const void *value) override
	{
                tid_check();
		const auto &k = *static_cast<const KeyType *>(key);
		const auto &v = *static_cast<const ValueType *>(value);

                BTreeOLCValue row;
                row.meta = MetaInitFunc()();
                row.value = v;

		bool success = btree.insert(k, row);
		CHECK(success == true);
	}

	void update(const void *key, const void *value, std::function<void(const void *, const void *)> on_update) override
	{
                tid_check();
		const auto &k = *static_cast<const KeyType *>(key);
		const auto &v = *static_cast<const ValueType *>(value);

                BTreeOLCValue *row_ptr;
                bool success = btree.lookup(k, row_ptr);
                CHECK(success == true);

		on_update(key, &row_ptr->value);
		row_ptr->value = v;
	}

	void deserialize_value(const void *key, StringPiece stringPiece) override
	{
		tid_check();
		std::size_t size = stringPiece.size();
		const auto &k = *static_cast<const KeyType *>(key);

		BTreeOLCValue *row_ptr;
                bool success = btree.lookup(k, row_ptr);
                CHECK(success == true);

		auto &v = row_ptr->value;

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

        void move_all_into_cxl(std::function<bool(ITable *, uint64_t, std::tuple<MetaDataType *, void *> &)> move_in_func) override
        {
                auto processor = [&](const KeyType &key, BTreeOLCValue &row, bool) -> bool {
                        MetaDataType *meta_ptr = &row.meta;
                        ValueType *data_ptr = &row.value;
                        std::tuple<MetaDataType *, void *> row_tuple(meta_ptr, data_ptr);
			bool ret = move_in_func(this, get_plain_key(&key), row_tuple);
                        CHECK(ret == true);
                        return true;
		};

                KeyType start_key;
                memset(&start_key, 0, sizeof(KeyType));
                btree.scanForUpdate(start_key, processor);
        }

    private:
	BTree btree;
	std::size_t tableID_;
	std::size_t partitionID_;
};

template <class KeyType, class ValueType> class HStoreTable : public ITable {
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

	std::tuple<MetaDataType *, void *> search(const void *key) override
	{
		const auto &k = *static_cast<const KeyType *>(key);
		auto &v = map_[k];
		return std::make_tuple(nullptr, &(v));
	}

	void *search_value(const void *key) override
	{
		const auto &k = *static_cast<const KeyType *>(key);
		return &map_[k];
	}

	MetaDataType &search_metadata(const void *key) override
	{
		static MetaDataType v;
		return v;
	}

	bool contains(const void *key) override
	{
		const auto &k = *static_cast<const KeyType *>(key);
		return map_.contains(k);
	}

	void insert(const void *key, const void *value) override
	{
		const auto &k = *static_cast<const KeyType *>(key);
		const auto &v = *static_cast<const ValueType *>(value);
		bool ok = map_.contains(k);
		DCHECK(ok == false);
		auto &row = map_[k];
		row = v;
	}

	void update(const void *key, const void *value, std::function<void(const void *, const void *)> on_update) override
	{
		const auto &k = *static_cast<const KeyType *>(key);
		const auto &v = *static_cast<const ValueType *>(value);
		auto &row = map_[k];
		on_update(key, &row);
		row = v;
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

    private:
	UnsafeHashMap<KeyType, ValueType> map_;
	std::size_t tableID_;
	std::size_t partitionID_;
};

template <std::size_t N, class KeyType, class ValueType> class HStoreCOWTable : public ITable {
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

	std::tuple<MetaDataType *, void *> search(const void *key) override
	{
		const auto &k = *static_cast<const KeyType *>(key);
		auto &v = map_[k];
		return std::make_tuple(nullptr, &(v));
	}

	void *search_value(const void *key) override
	{
		const auto &k = *static_cast<const KeyType *>(key);
		return &map_[k];
	}

	MetaDataType &search_metadata(const void *key) override
	{
		static MetaDataType v;
		return v;
	}

	bool contains(const void *key) override
	{
		const auto &k = *static_cast<const KeyType *>(key);
		return map_.contains(k);
	}

	void insert(const void *key, const void *value) override
	{
		const auto &k = *static_cast<const KeyType *>(key);
		const auto &v = *static_cast<const ValueType *>(value);
		bool ok = map_.contains(k);
		DCHECK(ok == false);
		auto &row = map_[k];
		std::get<1>(row) = v;
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

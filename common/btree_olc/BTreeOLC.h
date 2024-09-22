#pragma once
// Adapted from https://github.com/zxjcarrot/spitfire/blob/main/include/engine/btreeolc.h
// Contributors: Jie Hou, Yilin Chen, Xinjing ZHou
#include <immintrin.h>
#include <sched.h>

#include <atomic>
#include <cassert>
#include <cstring>
#include <functional>
#include <iostream>
#include <utility>
#include <vector>
#include <queue>
#include <thread>
#include <random>
#include <type_traits> // std::{enable_if,is_trivial}

#include "common/btree_olc/EBR.h"

extern thread_local uint32_t RWSpinLatchThreadId;

constexpr float kMergeThreshold = 0.4;
constexpr uint64_t kPageSize = 4096;
constexpr uint64_t kLeafPageSize = 4096;
constexpr uint64_t kLockMask = 1 << 10;
constexpr uint64_t kReaderMask = kLockMask - 1;
constexpr uint64_t kVersionMask = ~kReaderMask;

namespace btreeolc
{

struct LatchBase {
	std::atomic<uint64_t> word{ 0 };
};

class OLTPRWSpinLatch : public LatchBase {
    public:
	OLTPRWSpinLatch()
	{
		assert(RWSpinLatchThreadId > 0);
	}
	void acquireRead()
	{
		while (tryAcquireRead() == false) {
			_mm_pause();
		}
	}

	void acquireWrite()
	{
		while (tryAcquireWrite() == false) {
			_mm_pause();
		}
	}

	void releaseRead()
	{
		while (tryReleaseRead() == false) {
			_mm_pause();
		}
	}

	void releaseWrite()
	{
		while (tryReleaseWrite() == false) {
			_mm_pause();
		}
	}

	bool tryAcquireRead()
	{
		uint64_t w = word.load();
		uint64_t readerCount = getReaderCount(w);
		uint64_t writerId = getWriterId(w);
		if (writerId == 0) {
			uint64_t neww = makeNewWord(readerCount + 1, 0);
			return word.compare_exchange_strong(w, neww);
		}
		// Locked by other writer
		return false;
	}

	bool tryAcquireWrite()
	{
		uint64_t w = word.load();
		uint64_t readerCount = getReaderCount(w);
		uint64_t writerId = getWriterId(w);
		assert(RWSpinLatchThreadId > 0);
		if (writerId == 0 && readerCount == 0) {
			uint64_t neww = makeNewWord(readerCount, RWSpinLatchThreadId);
			return word.compare_exchange_strong(w, neww);
		}
		// Locked by other writer
		return false;
	}

    private:
	bool tryReleaseRead()
	{
		uint64_t w = word.load();
		uint64_t readerCount = getReaderCount(w);
		uint64_t writerId = getWriterId(w);
		assert(writerId == 0);
		assert(readerCount > 0);
		uint64_t neww = makeNewWord(readerCount - 1, 0);
		return word.compare_exchange_strong(w, neww);
	}

	bool tryReleaseWrite()
	{
		uint64_t w = word.load();
		uint64_t readerCount = getReaderCount(w);
		uint64_t writerId = getWriterId(w);
		assert(writerId == RWSpinLatchThreadId);
		assert(readerCount == 0);
		uint64_t neww = makeNewWord(0, 0);
		return word.compare_exchange_strong(w, neww);
	}

	inline uint64_t makeNewWord(uint32_t readerCount, uint32_t writerId)
	{
		uint64_t w = (((uint64_t)readerCount) << kReaderCountOffset) | (((uint64_t)writerId) << kWriterIdOffset);
		return w;
	}

	inline uint64_t getReaderCount(uint64_t w)
	{
		return (w >> kReaderCountOffset) & kReaderCountMask;
	}

	inline uint64_t getWriterId(uint64_t w)
	{
		return (w >> kWriterIdOffset) & kWriterIdMask;
	}

	static constexpr uint64_t kReaderCountOffset = 0;
	static constexpr uint64_t kReaderCountMask = 0xffffffff;
	static constexpr uint64_t kWriterIdOffset = 32;
	static constexpr uint64_t kWriterIdMask = 0xffffffff;
};

enum class RemovePredicateResult { GOOD = 0, VALUE_HAS_OTHER_REFERENCE, VALUE_NOT_SATISFYING_PREDICATE };
enum class RemoveResult { GOOD = 0, KEY_NOT_FOUND, VALUE_HAS_OTHER_REFERENCE, VALUE_NOT_SATISFYING_PREDICATE };

static inline void prefetch(char *ptr, size_t len)
{
	// if (ptr == nullptr) return;
	// for (char *p = ptr; p < ptr + len; p += 64) {
	//     __builtin_prefetch(p);
	// }
}

/**
 * class BPlusTree
 * This implementation assumes that KeyType has properly implemented default constructor, copy-constructor, assignment-constructor, destructor.
 * This class only manages the memory occupied by the KeyType object itself.
 * Therefore, KeyType should also properly implement deep-copying and destructor.
 * It will also use move-constructor if implemented.
 * @param KeyComparator a<b -1, a==b 0, a>b 1
 * @param ValueComparator a==b 0, a!=b 1
 */
template <class KeyType, class ValueType, class KeyComparator, class ValueComparator, std::size_t UpdateThreshold = 1024, uint64_t LeafPageSize = kLeafPageSize,
	  uint64_t InnerPageSize = kPageSize>
class BPlusTree {
    public:
	/** shorthand for the value list type */
	using ValueList = std::list<ValueType>;

	/** this is the element type of the leaf node */
	using KeyValuePair = std::pair<KeyType, ValueType>;

	/**
	 * enum class NodeType - B+ Tree node type
	 */
	enum class NodeType : uint8_t { BTreeInner = 1, BTreeLeaf = 2 };

	/**
	 * @brief NodeLock could either be a optimistc lock or a read-read spin lock.
	 * Its functionality is determined by the @RWLock parameter at construction.
	 */
	struct NodeLock : public LatchBase {
		// bool isRWLock = false;
	    public:
		NodeLock(bool isRWLock = false)
		{
			// this->isRWLock = isRWLock;
			// if (isRWLock) {
			//     word = 0;              // RW Spin Lock Initialzation
			// } else {
			word = kLockMask * 2; // Optimistic Lock Initialzation
			//}
		}

		bool isLocked(uint64_t version)
		{
			// assert(!isRWLock);
			return (version & kLockMask) != 0;
		}

		/**
		 * acquire success and increase counter
		 */
		uint64_t readLockOrRestart(bool &needRestart)
		{
			// if (isRWLock) {
			//     assert(getReaderCount(word.load()) < 20);
			//     acquireRead();
			//     assert(getReaderCount(word.load()) < 20);
			//     needRestart = false;
			//     //OLTP_LOG_INFO(RWSpinLatchThreadId," readLockOrRestart on ", (uint64_t)this,  ", Readers: ",getReaderCount(word.load()));
			//     return 0;
			// } else {
			uint64_t version;
			version = word.load(std::memory_order_relaxed);
			if (isLocked(version)) {
				// acquire read lock fail
				needRestart = true;
				_mm_pause();
			}
			return version;
			//}
		}

		void iteratorEnter(bool &needRestart)
		{
			// if (isRWLock) {
			//     assert(getReaderCount(word.load()) < 20);
			//     acquireRead();
			//     assert(getReaderCount(word.load()) < 20);
			// } else {
			uint64_t version;
			version = word.load(std::memory_order_relaxed);
			if (isLocked(version)) {
				// acquire read lock fail
				needRestart = true;
				_mm_pause();
				return;
			}
			word.fetch_add(1);
			//}
		}

		void iteratorLeave()
		{
			// if (isRWLock) {
			//     assert(getReaderCount(word.load()) < 20);
			//     releaseRead();
			//     assert(getReaderCount(word.load()) < 20);
			// } else {
			word.fetch_sub(1);
			//}
		}

		void writeLockOrRestart(bool &needRestart)
		{
			// if (isRWLock) {
			//     assert(getReaderCount(word.load()) < 20);
			//     acquireWrite();
			//     assert(getReaderCount(word.load()) < 20);
			// } else {
			uint64_t version;
			version = readLockOrRestart(needRestart);
			if (needRestart)
				return;

			upgradeToWriteLockOrRestart(version, needRestart);
			if (needRestart)
				return;
			//}
		}

		void upgradeToWriteLockOrRestart(uint64_t &version, bool &needRestart)
		{
			// if (isRWLock) {
			//     assert(getReaderCount(word.load()) < 20);
			//     needRestart = RWLockUpgradeToWriteLock() == false;
			//     assert(getReaderCount(word.load()) < 20);
			//     //OLTP_LOG_INFO(RWSpinLatchThreadId, " upgradeToWriteLockOrRestart on ", (uint64_t)this,  ", Readers:
			//     ",getReaderCount(word.load()));
			// } else {
			if ((word & kReaderMask) != 0) {
				needRestart = true;
				return;
			}
			if (word.compare_exchange_strong(version, version + kLockMask)) {
				version = version + kLockMask;
			} else {
				//_mm_pause();
				needRestart = true;
			}
			//}
		}

		void downgradeToReadLock(uint64_t &version)
		{
			// if (isRWLock) {
			//     assert(getReaderCount(word.load()) < 20);
			//     RWLockDowngradeToReadLock();
			//     assert(getReaderCount(word.load()) < 20);
			// } else {
			version = word.fetch_add(kLockMask);
			version += kLockMask;
			//}
		}

		void writeUnlock()
		{
			// if (isRWLock) {
			//     assert(getReaderCount(word.load()) < 20);
			//     //OLTP_LOG_INFO(RWSpinLatchThreadId, " writeUnlock on ", (uint64_t)this,  ", Readers: ",getReaderCount(word.load()));
			//     releaseWrite();
			//     assert(getReaderCount(word.load()) < 20);
			// } else {
			word.fetch_add(kLockMask);
			//}
		}

		void checkOrRestart(uint64_t startRead, bool &needRestart)
		{
			// if (isRWLock) {
			//     return;
			// }
			readUnlockOrRestart(startRead, needRestart, false);
		}

		void readUnlockOrRestart(uint64_t startRead, bool &needRestart, bool ebr = true)
		{
			// if (isRWLock) {
			//     assert(getReaderCount(word.load()) < 20);
			//     //OLTP_LOG_INFO(RWSpinLatchThreadId," readUnlockOrRestart on ", (uint64_t)this,  ", Readers: ",getReaderCount(word.load()));
			//     releaseRead();
			//     needRestart = false;
			// } else {
			needRestart = (((startRead ^ word.load()) & kVersionMask) != 0);
			//}
		}

		void acquireRead()
		{
			while (tryAcquireRead() == false) {
				_mm_pause();
			}
		}

		void acquireWrite()
		{
			while (tryAcqurieWrite() == false) {
				_mm_pause();
			}
		}

		void releaseRead()
		{
			while (tryReleaseRead() == false) {
				_mm_pause();
			}
		}

		void releaseWrite()
		{
			while (tryReleaseWrite() == false) {
				_mm_pause();
			}
		}

		bool tryAcquireRead()
		{
			uint64_t w = word.load();
			uint64_t readerCount = getReaderCount(w);
			uint64_t writerId = getWriterId(w);
			if (writerId == 0) {
				uint64_t neww = makeNewWord(readerCount + 1, 0);
				return word.compare_exchange_strong(w, neww);
			}
			// Locked by other writer
			return false;
		}

		bool RWLockUpgradeToWriteLock()
		{
			assert(getReaderCount(word.load()) < 20);
			// Only works if we are the only reader
			return tryUpgradeToWriteLock();
		}

		bool tryUpgradeToWriteLock()
		{
			uint64_t w = word.load();
			uint64_t readerCount = getReaderCount(w);
			uint64_t writerId = getWriterId(w);
			assert(writerId == 0); // Not locked
			assert(readerCount >= 1); // At least read locked by me
			if (readerCount > 1)
				return false;
			assert(readerCount == 1);
			uint64_t neww = makeNewWord(0, RWSpinLatchThreadId);
			return word.compare_exchange_strong(w, neww); // Try acquire write lock when we are the last reader
		}

		void RWLockDowngradeToReadLock()
		{
			while (tryDowngradeToReadLock() == false) {
				_mm_pause();
			}
		}

		bool tryDowngradeToReadLock()
		{
			uint64_t w = word.load();
			uint64_t readerCount = getReaderCount(w);
			uint64_t writerId = getWriterId(w);
			assert(writerId == RWSpinLatchThreadId); // Only write-locked by me
			assert(readerCount == 0);
			uint64_t neww = makeNewWord(1, 0);
			// word.store(neww);
			// return true;
			return word.compare_exchange_strong(w, neww); // Try acquire write lock when we are the last reader
		}

		bool tryAcqurieWrite()
		{
			uint64_t w = word.load();
			uint64_t readerCount = getReaderCount(w);
			uint64_t writerId = getWriterId(w);
			assert(RWSpinLatchThreadId > 0);
			if (writerId == 0 && readerCount == 0) {
				uint64_t neww = makeNewWord(readerCount, RWSpinLatchThreadId);
				return word.compare_exchange_strong(w, neww);
			}
			// Locked by other writer
			return false;
		}

		bool tryReleaseRead()
		{
			uint64_t w = word.load();
			uint64_t readerCount = getReaderCount(w);
			uint64_t writerId = getWriterId(w);
			assert(writerId == 0);
			assert(readerCount > 0);
			uint64_t neww = makeNewWord(readerCount - 1, 0);
			// word.fetch_sub(1);
			// return true;
			return word.compare_exchange_strong(w, neww);
		}

		bool tryReleaseWrite()
		{
			uint64_t w = word.load();
			uint64_t readerCount = getReaderCount(w);
			uint64_t writerId = getWriterId(w);
			assert(writerId == RWSpinLatchThreadId);
			assert(readerCount == 0);
			uint64_t neww = makeNewWord(0, 0);
			// word.store(0);
			// return true;
			return word.compare_exchange_strong(w, neww);
		}

		inline uint64_t makeNewWord(uint32_t readerCount, uint32_t writerId)
		{
			uint64_t w = (((uint64_t)readerCount) << kReaderCountOffset) | (((uint64_t)writerId) << kWriterIdOffset);
			return w;
		}

		inline uint64_t getReaderCount(uint64_t w)
		{
			return (w >> kReaderCountOffset) & kReaderCountMask;
		}

		inline uint64_t getWriterId(uint64_t w)
		{
			return (w >> kWriterIdOffset) & kWriterIdMask;
		}

		static constexpr uint64_t kReaderCountOffset = 0;
		static constexpr uint64_t kReaderCountMask = 0xffffffff;
		static constexpr uint64_t kWriterIdOffset = 32;
		static constexpr uint64_t kWriterIdMask = 0xffffffff;
	};

	/**
	 * class NodeMetaData - Holds node metadata
	 */
	class NodeMetaData {
	    public:
		/** node type */
		NodeType type_;

		/** child count */
		uint16_t count_;

		NodeMetaData(NodeType nodeType)
			: type_(nodeType)
			, count_(0)
		{
		}
	};

	/**
	 * class BaseNode - Generic node class; inherited by leaf, inner node
	 */
	class alignas(8) NodeBase : public NodeLock
	{
	    private:
		NodeMetaData meta_;

	    public:
		NodeBase(NodeType nodeType)
			: NodeLock(false)
			, meta_{ nodeType }
		{
		}
		NodeType getType() const
		{
			return meta_.type_;
		}
		uint16_t getCount() const
		{
			return meta_.count_;
		}
		void setCount(uint16_t count)
		{
			meta_.count_ = count;
		}
		// virtual int display(std::queue<NodeBase *> &nodeQueue) {}
	};

	/**
	 * We use `new[]` to allocate a block of memory, then use `placement new` to
	 * construct `BTreeLeaf/BTreeInner`. So when want to free the memory occupied
	 * by `BTreeLeaf/BTreeInner`, should use `delete[]`.
	 */
	struct Deallocator {
		void operator()(void *n) const
		{
			assert(((uint64_t)n) != 0xffffffffffffffffull);
			/*
			 * `n` maybe `NodeType *` or `char *`. If it's `NodeType *`, NO need to
			 * call its dtor manually beause there are no valid keys in that page.
			 */
			memset(n, 0xff, LeafPageSize);
			// delete[] reinterpret_cast<char *>(n);  // free memory
		}
	};

	/**
	 * class StackNodeElement - used when delete nodes recursively
	 */
	struct StackNodeElement {
		NodeBase *node;
		int pos;
		uint64_t version;
	};

	/**
	 * class BTreeLeaf - leaf node
	 */
	class BTreeLeaf : public NodeBase {
	    public:
		static constexpr uint64_t maxEntries = (LeafPageSize - sizeof(NodeBase) - sizeof(BTreeLeaf *) * 2) / (sizeof(KeyValuePair));
		static_assert(maxEntries >= 3, "maxEntries of BTreeLeaf must >= 3");

		BTreeLeaf *pre_;
		BTreeLeaf *next_;

		/** This is the array that we perform search on */
		KeyType keys_[maxEntries];
		ValueType values_[maxEntries];
		// KeyValuePair data_[0];

		BTreeLeaf()
			: NodeBase(NodeType::BTreeLeaf)
			, pre_(nullptr)
			, next_(nullptr)
		{
		}

		/**
		 * NOTE: used by tests
		 */
		int display(std::queue<NodeBase *> &nodeQueue)
		{
			assert(false);
			std::cout << "(";
			for (int i = 0; i < this->getCount(); i++) {
				// assert(data_[i].second != nullptr);
				// auto itr = data_[i].second->begin();
				// std::cout << data_[i].first.getValue();
				if (i != this->getCount() - 1) {
					std::cout << ",";
				}
			}
			std::cout << ") ";
			return 0;
		}

		/**
		 * flag is true, erase the <key, valueList>
		 * otherwise, erase the <key, value>
		 *
		 * return true if erase successfully
		 */
		bool erase(int pos, const KeyComparator &keyComp_, const ValueComparator &valueComp_, bool flag)
		{
			// assert(keyComp_(keys_[pos], element.first) == 0);
			bool res = false;

			__adjust_elements_in_erase(pos);

			res = true;
			this->setCount(this->getCount() - 1);
			return res;
		}
		/**
		 * @note Used when KeyType is trivial, e.g. `OLTPBtreeFixedLenKey`.
		 */
		template <typename T = KeyType> typename std::enable_if<std::is_trivial<T>::value == true, void>::type __adjust_elements_in_erase(int pos)
		{
			memmove(keys_ + pos, keys_ + pos + 1, sizeof(KeyType) * (this->getCount() - pos - 1));
			memmove(values_ + pos, values_ + pos + 1, sizeof(ValueType) * (this->getCount() - pos - 1));
			// for (uint16_t i = pos; i < this->getCount() - 1u; i++) {
			//     keys_[i] = keys_[i + 1];
			//     values_[i] = values_[i + 1];
			// }
			keys_[this->getCount() - 1].~KeyType();
			values_[this->getCount() - 1].~ValueType();
			// call dtor manually
		}
		/**
		 * @note Used when KeyType is non-trivial, e.g. `OLTPBtreeVarlenKey`.
		 */
		template <typename T = KeyType> typename std::enable_if<std::is_trivial<T>::value == false, void>::type __adjust_elements_in_erase(int pos)
		{
			assert(false);
			/*
			 * Here wants to erase key at `pos`, should transfer the pointer first.
			 */
			// char *ptr = data_[pos].first.transfer();

			// for (uint16_t i = pos; i < this->getCount() - 1u; i++) {
			//     data_[i].first = data_[i + 1].first;
			//     data_[i].second = data_[i + 1].second;
			// }
			// data_[this->getCount() - 1].~KeyValuePair();  // call dtor manually

			// /*
			//  * It is safe to delete the pointer here because already hold the
			//  * write lock of the leaf page.
			//  */
			// delete[] ptr;
		}

		bool hasEnoughSpace(int need)
		{
			return this->getCount() + need <= (int)maxEntries;
		}

		/**
		 * merge right `sibling`
		 * `sibling` need to be reclaimed
		 */
		void merge(BTreeLeaf *sibling)
		{
			assert(hasEnoughSpace(sibling->getCount()));
			for (uint16_t i = this->getCount(); i < sibling->getCount() + this->getCount(); i++) {
				new (&keys_[i]) KeyType{ sibling->keys_[i - this->getCount()] }; // Placement new
				new (&values_[i]) ValueType{ sibling->values_[i - this->getCount()] };
				sibling->keys_[i - this->getCount()].~KeyType(); // call dtor manually
				sibling->values_[i - this->getCount()].~ValueType();
			}
			this->setCount(this->getCount() + sibling->getCount());
			this->next_ = sibling->next_;
			assert(((uint64_t)sibling) != 0xffffffffffffffffull);
			EBR<UpdateThreshold, Deallocator>::getLocalThreadData().addRetiredNode(sibling);
		}

		bool needMerge()
		{
			return this->getCount() / (maxEntries + 0.0) < kMergeThreshold;
		}

		int getSurplus()
		{
			int threshold = std::ceil(maxEntries * kMergeThreshold);
			return this->getCount() - threshold;
		}

		bool isFull()
		{
			return this->getCount() == maxEntries;
		}

		ValueType getValue(const KeyType &k, const KeyComparator &keyComp_, std::function<ValueType(void)> createValue, bool &success)
		{
			assert(this->getCount() < maxEntries);
			unsigned pos = -1;
			pos = lowerBound(k, keyComp_);
			if (pos < this->getCount() && keyComp_(keys_[pos], k) == 0) {
				// already exists
				success = false;
				return values_[pos];
			} else {
				if (pos == this->getCount()) {
					// Placement new
					new (&keys_[this->getCount()]) KeyType{ k };
					new (&values_[this->getCount()]) ValueType{ createValue() };
				} else {
					// Placement new
					new (&keys_[this->getCount()]) KeyType{ keys_[this->getCount() - 1] };
					new (&values_[this->getCount()]) ValueType{ values_[this->getCount() - 1] };
					for (uint16_t i = this->getCount() - 1; i > pos; i--) {
						keys_[i] = keys_[i - 1];
						values_[i] = values_[i - 1];
					}
					keys_[pos] = k;
					values_[pos] = createValue();
				}
				this->setCount(this->getCount() + 1);
				success = true;
				return values_[pos];
			}
		}

		unsigned lowerBound(const KeyType &k, const KeyComparator &keyComp_)
		{
			if (this->getCount() < 128) {
				int left = 0;
				while (left < this->getCount() && keyComp_(this->keys_[left], k) < 0)
					++left;
				return left;
			} else {
				int left = 0, right = this->getCount() - 1;
				while (left <= right) {
					int mid = left + (right - left) / 2;
					int comp = keyComp_(keys_[mid], k);
					if (comp == 0)
						return mid;
					else if (comp < 0)
						left = mid + 1;
					else
						right = mid - 1;
				}
				return left;
			}
		}

		__attribute__((deprecated)) void update(const KeyType &k, ValueType p, unsigned pos)
		{
			assert(keyComp_(keys_[pos], k) == 0);
			values_[pos] = p;
		}

		const KeyType &max_key()
		{
			assert(this->getCount());
			return keys_[this->getCount() - 1];
		}

		// void insert_at(KeyType k, ValueType p, unsigned pos) {
		//     assert(keyComp_(data_[pos].first, k) == 0);
		//     if (this->getCount()) {
		//         for (uint16_t i = this->getCount(); i > pos; i--) {
		//             data_[i].first = data_[i - 1].first;
		//             data_[i].second = data_[i - 1].second;
		//         }
		//         data_[pos].first = k;
		//         data_[pos].second = p;
		//     } else {
		//         data_[0].first = k;
		//         data_[0].second = p;
		//     }
		//     setCount(this->getCount() + 1);
		// }

		/**
		 * return v (in the b+tree) and false , if <k, v> already exists
		 * otherwise return v and true
		 */
		ValueType insert(const KeyType &k, ValueType v, const KeyComparator &keyComp_, bool &success)
		{
			assert(this->getCount() < maxEntries);
			unsigned pos = -1;
			pos = lowerBound(k, keyComp_);
			if (pos < this->getCount() && keyComp_(keys_[pos], k) == 0) {
				// already exists
				success = false;
				return values_[pos];
			} else {
				if (pos == this->getCount()) {
					// Placement new
					new (&keys_[this->getCount()]) KeyType{ k };
					new (&values_[this->getCount()]) ValueType{ v };
				} else {
					// Placement new
					new (&keys_[this->getCount()]) KeyType{ keys_[this->getCount() - 1] };
					new (&values_[this->getCount()]) ValueType{ values_[this->getCount() - 1] };
					for (uint16_t i = this->getCount() - 1; i > pos; i--) {
						keys_[i] = keys_[i - 1];
						values_[i] = values_[i - 1];
					}
					keys_[pos] = k;
					values_[pos] = v;
				}
				this->setCount(this->getCount() + 1);
				success = true;
				return v;
			}
		}

		/**
		 * return v (in the b+tree) and false , if <k, v> already exists
		 * otherwise return v and true
		 */
		bool insert(const KeyType &k, const ValueType &v, const KeyComparator &keyComp_)
		{
			assert(this->getCount() < maxEntries);
			unsigned pos = -1;
			pos = lowerBound(k, keyComp_);
			if (pos < this->getCount() && keyComp_(keys_[pos], k) == 0) {
				// already exists
				return false;
			} else {
				if (pos == this->getCount()) {
					// Placement new
					new (&keys_[this->getCount()]) KeyType{ k };
					new (&values_[this->getCount()]) ValueType{ v };
				} else {
					// Placement new
					new (&keys_[this->getCount()]) KeyType{ keys_[this->getCount() - 1] };
					new (&values_[this->getCount()]) ValueType{ values_[this->getCount() - 1] };
					for (uint16_t i = this->getCount() - 1; i > pos; i--) {
						keys_[i] = keys_[i - 1];
						values_[i] = values_[i - 1];
					}
					keys_[pos] = k;
					values_[pos] = v;
				}
				this->setCount(this->getCount() + 1);
				return true;
			}
		}

		/**
		 * split leaf node
		 */
		BTreeLeaf *split(KeyType &sep)
		{
			char *base = new char[sizeof(BTreeLeaf)];
			BTreeLeaf *newLeaf = new (base) BTreeLeaf(); // Placement new

			newLeaf->setCount(this->getCount() - (this->getCount() / 2));
			newLeaf->next_ = next_;
			newLeaf->pre_ = this;

			this->setCount(this->getCount() - newLeaf->getCount());
			next_ = newLeaf;

			for (uint16_t i = 0; i < newLeaf->getCount(); i++) {
				new (&newLeaf->keys_[i]) KeyType{ keys_[i + this->getCount()] }; // Placement new
				new (&newLeaf->values_[i]) ValueType{ values_[i + this->getCount()] }; // Placement new
				keys_[i + this->getCount()].~KeyType();
				values_[i + this->getCount()].~ValueType(); // call dtor manually
			}

			__get_separate_key_in_split(sep);

			return newLeaf;
		}
		/**
		 * @note Used when KeyType is trivial, e.g. `OLTPBtreeFixedLenKey`.
		 */
		template <typename T = KeyType> typename std::enable_if<std::is_trivial<T>::value == true, void>::type __get_separate_key_in_split(KeyType &sep)
		{
			sep = keys_[this->getCount() - 1];
		}
		/**
		 * @note Used when KeyType is non-trivial, e.g. `OLTPBtreeVarlenKey`.
		 */
		template <typename T = KeyType>
		typename std::enable_if<std::is_trivial<T>::value == false, void>::type __get_separate_key_in_split(KeyType &sep)
		{
			assert(false);
			/*
			 * separate key will push up to the node's parent, so here should
			 * deep copy the key, because do NOT allow two non-trivial KeyType
			 * share the same pointer, otherwise will cause "double free".
			 */
			sep = keys_[this->getCount() - 1].deepCopy();
		}
	};
	// static_assert(LeafPageSize > sizeof(BTreeLeaf), "LeafPageSize too small");

	/**
	 * BTreeInner - inner node
	 */
	class BTreeInner : public NodeBase {
	    public:
		static constexpr uint64_t maxEntries = (InnerPageSize - sizeof(NodeBase)) / (sizeof(KeyType) + sizeof(NodeBase *));
		static_assert(maxEntries >= 3, "maxEntries of BTreeInner must >= 3");

		static constexpr uint64_t childOffset = maxEntries * sizeof(KeyType);

		/**
		 * Layout of `data_`:
		 *  -------------------------------------------------------------------
		 *  | Key 0 | Key 1 | ... | Key N | Child 0 | Child 1 | ... | Child N |
		 *  -------------------------------------------------------------------
		 */
		char data_[0];

		BTreeInner()
			: NodeBase(NodeType::BTreeInner)
		{
		}

		KeyType &keyAt(size_t i)
		{
			return *reinterpret_cast<KeyType *>(reinterpret_cast<intptr_t>(data_) + i * sizeof(KeyType));
		}

		NodeBase *&childAt(size_t i)
		{
			return *reinterpret_cast<NodeBase **>(reinterpret_cast<intptr_t>(data_) + childOffset + i * sizeof(NodeBase *));
		}

		void newKey(const size_t pos, const KeyType &key)
		{
			new (&keyAt(pos)) KeyType{ key }; // Placement new
		}

		bool isFull()
		{
			return this->getCount() == (maxEntries - 1);
		}

		/**
		 * erase the key at `pos` and the child at `pos + 1`
		 * return whether a merge opt is required
		 * When the type and number of nodes are InnerNode and 0 respectively,
		 * you need to change root node, old root node need to be reclaimed
		 *
		 * NOTE: return true, if need to change root node
		 */
		bool erase(int pos)
		{
			__adjust_elements_in_erase(pos);

			this->setCount(this->getCount() - 1);
			return this->getCount() == 0;
		}
		/**
		 * @note Used when KeyType is trivial, e.g. `OLTPBtreeFixedLenKey`.
		 */
		template <typename T = KeyType> typename std::enable_if<std::is_trivial<T>::value == true, void>::type __adjust_elements_in_erase(int pos)
		{
			for (int i = pos; i < this->getCount() - 1; i++) {
				keyAt(i) = keyAt(i + 1);
			}
			keyAt(this->getCount() - 1).~KeyType(); // call dtor manually

			// always merge nodes to the left node, so remove the `pos + 1` child
			memmove(&childAt(pos + 1), &childAt(pos + 2), sizeof(NodeBase *) * (this->getCount() - pos - 1));
		}
		/**
		 * @note Used when KeyType is non-trivial, e.g. `OLTPBtreeVarlenKey`.
		 */
		template <typename T = KeyType> typename std::enable_if<std::is_trivial<T>::value == false, void>::type __adjust_elements_in_erase(int pos)
		{
			assert(false);
			/*
			 * Here wants to erase key at `pos`, should transfer the pointer first.
			 */
			char *ptr = keyAt(pos).transfer();

			for (int i = pos; i < this->getCount() - 1; i++) {
				keyAt(i) = keyAt(i + 1);
			}
			keyAt(this->getCount() - 1).~KeyType(); // call dtor manually

			/*
			 * Because of OLC strategy, can NOT delete pointer here, should transfer
			 * the pointer to EBR, it will delete the pointer safely.
			 */
			assert(((uint64_t)ptr) != 0xffffffffffffffffull);
			EBR<UpdateThreshold, Deallocator>::getLocalThreadData().addRetiredNode(ptr);

			// always merge nodes to the left node, so remove the `pos + 1` child
			memmove(&childAt(pos + 1), &childAt(pos + 2), sizeof(NodeBase *) * (this->getCount() - pos - 1));
		}

		void merge(BTreeInner *sibling, const KeyType &subTreeMaxKey)
		{
			__adjust_elements_in_merge(sibling, subTreeMaxKey);

			this->setCount(this->getCount() + 1);
			this->setCount(this->getCount() + sibling->getCount());

			assert(((uint64_t)sibling) != 0xffffffffffffffffull);
			EBR<UpdateThreshold, Deallocator>::getLocalThreadData().addRetiredNode(sibling);
		}
		/**
		 * @note Used when KeyType is trivial, e.g. `OLTPBtreeFixedLenKey`.
		 */
		template <typename T = KeyType>
		typename std::enable_if<std::is_trivial<T>::value == true, void>::type __adjust_elements_in_merge(BTreeInner *sibling,
														  const KeyType &subTreeMaxKey)
		{
			newKey(this->getCount(), subTreeMaxKey);

			for (uint16_t i = 0; i < sibling->getCount(); i++) {
				newKey(this->getCount() + 1 + i, sibling->keyAt(i));
				sibling->keyAt(i).~KeyType(); // call dtor manually
			}

			memmove(&childAt(this->getCount() + 1), &sibling->childAt(0), sizeof(NodeBase *) * (sibling->getCount() + 1));
		}
		/**
		 * @note Used when KeyType is non-trivial, e.g. `OLTPBtreeVarlenKey`.
		 */
		template <typename T = KeyType>
		typename std::enable_if<std::is_trivial<T>::value == false, void>::type __adjust_elements_in_merge(BTreeInner *sibling,
														   const KeyType &subTreeMaxKey)
		{
			/*
			 * Here should deep copy `subTreeMaxKey`, because do NOT allow two
			 * non-trivial KeyType share the same pointer, otherwise will cause
			 * "double free".
			 */
			newKey(this->getCount(), subTreeMaxKey.deepCopy());

			for (uint16_t i = 0; i < sibling->getCount(); i++) {
				newKey(this->getCount() + 1 + i, sibling->keyAt(i));
				sibling->keyAt(i).~KeyType(); // call dtor manually
			}

			memmove(&childAt(this->getCount() + 1), &sibling->childAt(0), sizeof(NodeBase *) * (sibling->getCount() + 1));
		}

		__attribute__((deprecated)) void updateKey(const KeyType &oldKey, const KeyType &newKey, const KeyComparator &keyComp_)
		{
			unsigned pos = lowerBound(oldKey, keyComp_);
			assert(pos >= 0 && pos < this->getCount());
			keyAt(pos) = newKey;
		}

		__attribute__((deprecated)) void removeKey(const KeyType &key, const KeyComparator &keyComp_)
		{
			unsigned pos = lowerBound(key, keyComp_);
			assert(pos >= 0 && pos < this->getCount());
			for (uint16_t i = pos; i < this->getCount() - 1u; i++) {
				keyAt(i) = keyAt(i + 1);
			}
			keyAt(this->getCount() - 1u).~KeyType(); // call dtor manually

			memmove(&childAt(pos + 1), &childAt(pos + 2), sizeof(KeyType) * (this->getCount() - pos + 1));
			setCount(this->getCount() - 1);
		}

		bool hasEnoughSpace(int need)
		{
			return this->getCount() + need + 1 < (int)maxEntries;
		}

		unsigned lowerBound(const KeyType &k, const KeyComparator &keyComp_)
		{
			if (this->getCount() < 128) {
				int left = 0;
				while (left < this->getCount() && keyComp_(keyAt(left), k) < 0)
					++left;
				return left;
			} else {
				int left = 0, right = this->getCount() - 1;
				while (left <= right) {
					int mid = left + (right - left) / 2;
					int comp = keyComp_(keyAt(mid), k);
					if (comp == 0)
						return mid;
					else if (comp < 0)
						left = mid + 1;
					else
						right = mid - 1;
				}
				return left;
			}
		}

		bool needMerge()
		{
			return this->getCount() / (maxEntries + 0.0) < kMergeThreshold;
		}

		int getSurplus()
		{
			int threshold = std::ceil(maxEntries * kMergeThreshold);
			return this->getCount() - threshold;
		}

		BTreeInner *split(KeyType &sep)
		{
			char *base = new char[InnerPageSize];
			BTreeInner *newInner = new (base) BTreeInner(); // Placement new

			newInner->setCount(this->getCount() - (this->getCount() / 2));
			this->setCount(this->getCount() - newInner->getCount() - 1);

			sep = keyAt(this->getCount());
			/*
			 * separate key will push up to the node's parent,
			 * so need to destruct separate key here and NO need
			 * to transfer pointer.
			 */
			keyAt(this->getCount()).~KeyType(); // call dtor manually

			for (uint16_t i = 0; i < newInner->getCount(); i++) {
				newInner->newKey(i, keyAt(this->getCount() + 1 + i));
				keyAt(this->getCount() + 1 + i).~KeyType(); // call dtor manually
			}
			memcpy(&newInner->childAt(0), &childAt(this->getCount() + 1), sizeof(NodeBase *) * (newInner->getCount() + 1));

			return newInner;
		}

		const KeyType &max_key()
		{
			assert(this->getCount());
			return keyAt(this->getCount() - 1);
		}

		void insert(const KeyType &k, NodeBase *child, const KeyComparator &keyComp_)
		{
			assert(this->getCount() < maxEntries - 1);
			unsigned pos = lowerBound(k, keyComp_);
			if (pos == this->getCount()) {
				newKey(this->getCount(), k);
			} else {
				newKey(this->getCount(), keyAt(this->getCount() - 1));
				for (uint16_t i = this->getCount() - 1; i > pos; i--) {
					keyAt(i) = keyAt(i - 1);
				}
				keyAt(pos) = k;
			}

			memmove(&childAt(pos + 1), &childAt(pos), sizeof(NodeBase *) * (this->getCount() - pos + 1));
			childAt(pos) = child;

			std::swap(childAt(pos), childAt(pos + 1));
			this->setCount(this->getCount() + 1);
		}

		/**
		 * NOTE: used by tests
		 */
		int display(std::queue<NodeBase *> &nodeQueue)
		{
			std::cout << "[";
			for (int i = 0; i < this->getCount(); i++) {
				std::cout << keyAt(i).getValue();
				if (i != this->getCount() - 1) {
					std::cout << '|';
				}
			}
			std::cout << "] ";

			if (this->getCount()) {
				for (int i = 0; i <= this->getCount(); i++) {
					nodeQueue.push(childAt(i));
				}
				return this->getCount() + 1;
			} else {
				return 0;
			}
		}
	};
	static_assert(InnerPageSize > sizeof(BTreeInner), "InnerPageSize too small");

	/**
	 * NOTE: developing
	 */
	class BPlusTreeIterator {
		/** Enum that represents the state of the iterator */
		enum IteratorState { VALID, END, REND, RETRY1, INVALID };

		BTreeLeaf *curNode_;
		int curPos_;
		IteratorState state_;

	    public:
		BPlusTreeIterator()
			: curNode_(nullptr)
			, curPos_(-1)
			, state_(VALID)
		{
		}

		~BPlusTreeIterator()
		{
			if (curNode_) {
				curNode_->iteratorLeave();
			}
		}

		/**
		 * Assuming that curNode has increase the number of readers by one
		 */
		BPlusTreeIterator(BTreeLeaf *curNode, int curPos)
			: curNode_(curNode)
			, curPos_(curPos)
		{
			if (curNode_ == nullptr || curNode_->getCount() == curPos) {
				setEndIterator(true);
				return;
			}
			state_ = IteratorState::VALID;
			assert(curPos >= 0);
		}

		BPlusTreeIterator &operator=(const BPlusTreeIterator &) = delete;

		void operator++(int)
		{
			forward();
		}

		void operator++()
		{
			forward();
		}

		void operator--(int)
		{
			backward();
		}

		void operator--()
		{
			backward();
		}

		/**
		 * Equality operator to check if two iterators are equal
		 * @param itr Iterator to be compared with this iterator
		 * @return true if iterators are equal, false otherwise
		 *
		 * NOTE: developing
		 */
		bool operator==(const BPlusTreeIterator &itr)
		{
			bool result = (curNode_ == itr.curNode_ && curPos_ == itr.curPos_ && state_ == itr.state_);
			return result;
		}

		bool operator!=(const BPlusTreeIterator &itr)
		{
			bool result = (curNode_ == itr.curNode_ && curPos_ == itr.curPos_ && state_ == itr.state_);
			return !result;
		}

		const KeyType &key() const
		{
			return curNode_->keys_[curPos_];
		}

		const ValueType &value() const
		{
			return curNode_->values_[curPos_];
		}

		/**
		 * Resets the iterator to represent a dummy iterator.
		 */
		void resetIterator(bool itrLeave)
		{
			if (curNode_ && itrLeave) {
				curNode_->iteratorLeave();
			}
			curNode_ = nullptr;
			curPos_ = -1;
		}

		/**
		 * Sets the state of the iterator to END iterator
		 */
		void setEndIterator(bool itrLeave = false)
		{
			resetIterator(itrLeave);
			state_ = IteratorState::END;
		}

		/**
		 * Sets the state of the iterator to RETRY iterator
		 */
		void setRetryIterator(bool itrLeave = false)
		{
			resetIterator(itrLeave);
			state_ = IteratorState::RETRY1;
		}

		/**
		 * Returns End() iterator
		 */
		static BPlusTreeIterator getEndIterator()
		{
			auto iterator = BPlusTreeIterator();
			iterator.setEndIterator();
			return iterator;
		}

		/**
		 * Returns Retry() iterator
		 */
		static BPlusTreeIterator getRetryIterator()
		{
			auto iterator = BPlusTreeIterator();
			iterator.setRetryIterator();
			return iterator;
		}

	    private:
		/**
		 * called when iterator move to the next leaf node
		 */
		void forward()
		{
			assert(state_ == IteratorState::VALID);
			bool needRestart = false;
			curPos_++;
			// move to next leafNode
			if (curPos_ >= static_cast<int>(curNode_->getCount())) {
				if (curNode_->next_ == nullptr) {
					setEndIterator(true);
					return;
				}
				auto preNode = curNode_;
				curNode_ = curNode_->next_;
				curNode_->iteratorEnter(needRestart);
				if (needRestart) {
					preNode->iteratorLeave();
					setRetryIterator();
					return;
				}
				preNode->iteratorLeave();
				curPos_ = 0;
			}
		}

		/**
		 * called when iterator move to the previous leaf node
		 */
		void backward()
		{
			assert(state_ == IteratorState::VALID);
			bool needRestart = false;
			curPos_--;
			// move to previous leafNode
			if (curPos_ < 0) {
				if (curNode_->pre_ == nullptr) {
					setEndIterator(true);
					return;
				}
				auto preNode = curNode_;
				curNode_ = curNode_->pre_;
				curNode_->iteratorEnter(needRestart);
				if (needRestart) {
					preNode->iteratorLeave();
					setRetryIterator();
					return;
				}
				preNode->iteratorLeave();
				curPos_ = curNode_->getCount() - 1;
			}
		}
	};

	/**
	 * NOTE: used by tests
	 */
	void display()
	{
		NodeBase *node = root_;
		// std::cout << node->getCount() << std::endl;
		std::queue<NodeBase *> nodeQueue;

		std::cout << "++++++\n";
		int p_sum;
		if (node->getType() == NodeType::BTreeLeaf) {
			p_sum = reinterpret_cast<BTreeLeaf *>(node)->display(nodeQueue);
		} else {
			p_sum = reinterpret_cast<BTreeInner *>(node)->display(nodeQueue);
		}

		std::cout << std::endl;
		int sum = 0;
		while (nodeQueue.empty() == false) {
			for (int i = 0; i < p_sum; i++) {
				node = nodeQueue.front();
				nodeQueue.pop();

				if (node->getType() == NodeType::BTreeLeaf) {
					sum += reinterpret_cast<BTreeLeaf *>(node)->display(nodeQueue);
				} else {
					sum += reinterpret_cast<BTreeInner *>(node)->display(nodeQueue);
				}
			}
			std::cout << std::endl;
			p_sum = sum;
			sum = 0;
		}
		std::cout << "------\n";
	}

	BPlusTree(bool isUnique = false, const KeyComparator &keyComp = KeyComparator{}, const ValueComparator &valueComp = ValueComparator{})
		: keyComp_(keyComp)
		, valueComp_(valueComp)
		, keyUnique_(isUnique)
	{
		char *base = new char[LeafPageSize];
		root_ = new (base) BTreeLeaf(); // Placement new
		stats_.leaf_nodes++;
		// std::cout << "BTreeLeaf::maxEntries = " << BTreeLeaf::maxEntries << ", "
		//           << "BTreeInner::maxEntries = " << BTreeInner::maxEntries << ".\n";
	}

	void makeRoot(const KeyType &k, NodeBase *leftChild, NodeBase *rightChild)
	{
		char *base = new char[InnerPageSize];
		auto inner = new (base) BTreeInner(); // Placement new

		inner->setCount(1);
		inner->newKey(0, k);
		inner->childAt(0) = leftChild;
		inner->childAt(1) = rightChild;
		root_ = inner;
	}

	void destroy(NodeBase *node)
	{
		if (node == nullptr)
			return;
		if (node->getType() == NodeType::BTreeInner) {
			auto inner = static_cast<BTreeInner *>(node);
			for (size_t i = 0; i <= inner->getCount(); ++i) {
				if (i != inner->getCount()) {
					inner->keyAt(i).~KeyType(); // call dtor manually
				}
				destroy(inner->childAt(i));
			}
			inner->~BTreeInner();
			delete[] reinterpret_cast<char *>(inner);
		} else {
			auto leaf = static_cast<BTreeLeaf *>(node);
			for (size_t i = 0; i < leaf->getCount(); ++i) {
				leaf->keys_[i].~KeyType(); // call dtor manually
				leaf->values_[i].~ValueType();
			}
			leaf->~BTreeLeaf();
			delete[] reinterpret_cast<char *>(leaf);
		}
	}

	~BPlusTree()
	{
		destroy(root_.load());
	}

	int intRand(const int &min, const int &max)
	{
		static thread_local std::mt19937 generator;
		std::uniform_int_distribution<int> distribution(min, max);
		return distribution(generator);
	}

	void yield(int count, bool wait = false)
	{
		if (count > 10 && wait) { // too many collisions/contention, switch to randomized waits (TODO: exponential back-off)
#ifndef WINDOWS
			auto us = intRand(0, 20);
			std::this_thread::sleep_for(std::chrono::microseconds(us));
#else
			// TODO in windows
#endif
		} else if (count > 3) {
#ifndef WINDOWS
			sched_yield();
#endif
		} else {
			_mm_pause();
		}
	}

	bool hasEnoughSpace(NodeBase *node, int count)
	{
		if (node->getType() == NodeType::BTreeInner) {
			return static_cast<BTreeInner *>(node)->hasEnoughSpace(count);
		} else {
			return static_cast<BTreeLeaf *>(node)->hasEnoughSpace(count);
		}
	}

	bool needMerge(NodeBase *node, int count)
	{
		if (node->getType() == NodeType::BTreeInner) {
			return static_cast<BTreeInner *>(node)->needMerge();
		} else {
			return static_cast<BTreeLeaf *>(node)->needMerge();
		}
	}

	int getSurplus(NodeBase *node)
	{
		if (node->getType() == NodeType::BTreeInner) {
			return static_cast<BTreeInner *>(node)->getSurplus();
		} else {
			return static_cast<BTreeLeaf *>(node)->getSurplus();
		}
	}
	/**
	 * borrow data from sibling
	 * when borrow from left, opt is 0
	 * when borrow from right, opt is 1
	 * `pos`: the left node pos in the parent node
	 *        needed when update parent node
	 */
	void reallocNode(NodeBase *left, NodeBase *right, int opt, BTreeInner *p, unsigned pos)
	{
		if (left->getType() == NodeType::BTreeLeaf) {
			auto a = static_cast<BTreeLeaf *>(left);
			auto b = static_cast<BTreeLeaf *>(right);
			if (opt) {
				/*
				 * left borrow one from right
				 *
				 *        A                              C
				 *      /   \                         /     \
				 *  [ B ]   [ C  D ]     ==>      [ B  C ]  [ D ]
				 *    |       |  |                  |  |      |
				 *    b       c  d                  b  c      d
				 */
				// adjust left node
				new (&a->keys_[a->getCount()]) KeyType{ b->keys_[0] }; // Placement new
				new (&a->values_[a->getCount()]) ValueType{ b->values_[0] }; // Placement new
				a->setCount(a->getCount() + 1);

				// adjust right node
				for (uint16_t i = 0; i < b->getCount() - 1; i++) {
					b->keys_[i] = b->keys_[i + 1];
					b->values_[i] = b->values_[i + 1];
				}
				b->keys_[b->getCount() - 1].~KeyType(); // call dtor manually
				b->values_[b->getCount() - 1].~ValueType();
				b->setCount(b->getCount() - 1);
			} else {
				/*
				 * right borrow one from left
				 *
				 *         A                           B
				 *      /     \                     /     \
				 *  [ B  C ]  [ D ]     ==>      [ B ]   [ C D ]
				 *    |  |      |                  |       |  |
				 *    b  c      d                  b       c  d
				 */
				// move right
				for (uint16_t i = b->getCount(); i > 0; i--) {
					new (&b->keys_[i]) KeyType{ b->keys_[i - 1] };
					new (&b->values_[i]) ValueType{ b->values_[i - 1] };
					b->keys_[i - 1].~KeyType();
					b->values_[i - 1].~ValueType();
				}
				b->setCount(b->getCount() + 1);
				a->setCount(a->getCount() - 1);
				for (int i = 0; i < 1; i++) {
					new (&b->keys_[i]) KeyType{ a->keys_[a->getCount() + i] };
					new (&b->values_[i]) ValueType{ a->values_[a->getCount() + i] };
					a->keys_[a->getCount() + i].~KeyType();
					a->values_[a->getCount() + i].~ValueType();
				}
			}
			// adjust parent: replace the parent key which in the `pos`
			__adjust_parent_in_reallocNode(p, pos, a->max_key());
		} else {
			auto a = static_cast<BTreeInner *>(left);
			auto b = static_cast<BTreeInner *>(right);
			if (opt) {
				/*
				 * left borrow one from right
				 *
				 *        A                               C
				 *      /   \                         /       \
				 *  [ B ]   [ C  D ]     ==>      [B  A ]    [ D ]
				 *  /  \    /  \  \              /  \  \     /  \
				 *  a    b  c    d   e          a    b  c   d    e
				 */
				// adjust left node
				a->setCount(a->getCount() + 1);
				a->newKey(a->getCount() - 1, p->keyAt(pos));
				a->childAt(a->getCount()) = b->childAt(0);

				// adjust parent
				p->keyAt(pos) = b->keyAt(0);

				// adjust right node
				memmove(&b->childAt(0), &b->childAt(1), sizeof(NodeBase *) * (b->getCount()));
				for (uint16_t i = 0; i < b->getCount() - 1; i++) {
					b->keyAt(i) = b->keyAt(i + 1);
				}
				b->keyAt(b->getCount() - 1).~KeyType(); // call dtor manually
				b->setCount(b->getCount() - 1);
			} else {
				/*
				 * right borrow one from left
				 *
				 *         A                             C
				 *      /     \                       /     \
				 *  [ B C ]   [ D ]      ==>      [ B ]    [ A D ]
				 *  /  \  \    / \                 /  \     / / \
				 *  a   b  c  d   e               a   b    c d   e
				 */
				// adjust right node
				memmove(&b->childAt(1), &b->childAt(0), sizeof(NodeBase *) * (b->getCount() + 1));
				for (int i = b->getCount(); i > 0; i--) {
					b->newKey(i, b->keyAt(i - 1));
					b->keyAt(i - 1).~KeyType();
				}
				b->newKey(0, p->keyAt(pos));
				b->childAt(0) = a->childAt(a->getCount());
				b->setCount(b->getCount() + 1);

				// adjust parent
				p->keyAt(pos) = a->keyAt(a->getCount() - 1);

				// adjust left node
				a->keyAt(a->getCount() - 1).~KeyType();
				a->setCount(a->getCount() - 1);
			}
		}
	}
	/**
	 * @note Used when KeyType is trivial, e.g. `OLTPBtreeFixedLenKey`.
	 */
	template <typename T = KeyType>
	typename std::enable_if<std::is_trivial<T>::value == true, void>::type __adjust_parent_in_reallocNode(BTreeInner *p, unsigned pos, const KeyType &key)
	{
		p->keyAt(pos) = key;
	}
	/**
	 * @note Used when KeyType is non-trivial, e.g. `OLTPBtreeVarlenKey`.
	 */
	template <typename T = KeyType>
	typename std::enable_if<std::is_trivial<T>::value == false, void>::type __adjust_parent_in_reallocNode(BTreeInner *p, unsigned pos, const KeyType &key)
	{
		assert(false);
		char *ptr = p->keyAt(pos).transfer();

		p->keyAt(pos) = key.deepCopy();

		assert(((uint64_t)ptr) != 0xffffffffffffffffull);
		EBR<UpdateThreshold, Deallocator>::getLocalThreadData().addRetiredNode(ptr);
	}

	/**
	 * return v if insert successful
	 * otherwise return an existing value
	 */
	bool insert(const KeyType &k, const ValueType &v)
	{
		// EBR<UpdateThreshold, Deallocator>::getLocalThreadData().enterCritical();
		// btreeolc::DeferCode c([]() { EBR<UpdateThreshold, Deallocator>::getLocalThreadData().leaveCritical(); });
		int restartCount = 0;
restart:
		// need yield CPU when come here at second time
		if (restartCount++)
			yield(restartCount, true);
		bool needRestart = false;

		// Current node
		NodeBase *node = root_;
		uint64_t versionNode = node->readLockOrRestart(needRestart);
		if (needRestart || (node != root_)) {
			node->readUnlockOrRestart(versionNode, needRestart);
			goto restart;
		}

		// Parent of current node
		BTreeInner *parent = nullptr;
		uint64_t versionParent = 0;

		while (node->getType() == NodeType::BTreeInner) {
			auto inner = static_cast<BTreeInner *>(node);
			// Split eagerly if full
			if (inner->isFull()) {
				// Lock
				if (parent) {
					parent->upgradeToWriteLockOrRestart(versionParent, needRestart);
					if (needRestart)
						goto restart;
				}
				node->upgradeToWriteLockOrRestart(versionNode, needRestart);
				if (needRestart) {
					if (parent)
						parent->writeUnlock();
					goto restart;
				}
				// parent is null and node isn't root
				if (!parent && (node != root_)) {
					// there's a new parent
					node->writeUnlock();
					goto restart;
				}
				// Split
				KeyType sep;
				BTreeInner *newInner = inner->split(sep);
				stats_.inner_nodes++;
				if (parent)
					parent->insert(sep, newInner, keyComp_);
				else
					makeRoot(sep, inner, newInner);

				// Unlock and restart
				node->writeUnlock();
				if (parent) {
					parent->writeUnlock();
				}
				goto restart;
			}

			if (parent) {
				parent->readUnlockOrRestart(versionParent, needRestart);
				if (needRestart)
					goto restart;
			}

			parent = inner;
			versionParent = versionNode;

			node = inner->childAt(inner->lowerBound(k, keyComp_));
			// prefetch((char *)node, sizeof(NodeMetaData));
			inner->checkOrRestart(versionNode, needRestart);
			if (needRestart)
				goto restart;

			versionNode = node->readLockOrRestart(needRestart);
			if (needRestart)
				goto restart;
		} // while

		if (parent) {
			parent->checkOrRestart(versionParent, needRestart);
			if (needRestart) {
				node->readUnlockOrRestart(versionNode, needRestart);
				goto restart;
			}
		}
		auto leaf = static_cast<BTreeLeaf *>(node);
		ValueType insertRes;
		bool success;

		// Split leaf if full
		if (leaf->getCount() == leaf->maxEntries) {
			// Lock
			if (parent) {
				parent->upgradeToWriteLockOrRestart(versionParent, needRestart);
				if (needRestart) {
					goto restart;
				}
			}
			node->upgradeToWriteLockOrRestart(versionNode, needRestart);
			if (needRestart) {
				if (parent)
					parent->writeUnlock();
				goto restart;
			}
			if (!parent && (node != root_)) {
				// there's a new parent
				node->writeUnlock();
				goto restart;
			}
			// Split
			KeyType sep;
			BTreeLeaf *newLeaf = leaf->split(sep);
			stats_.leaf_nodes++;
			if (keyComp_(k, sep) > 0) {
				success = newLeaf->insert(k, v, keyComp_);
			} else {
				success = leaf->insert(k, v, keyComp_);
			}

			if (success) {
				stats_.num_items++;
			}

			if (parent)
				parent->insert(sep, newLeaf, keyComp_);
			else
				makeRoot(sep, leaf, newLeaf);
			// Unlock and restart
			node->writeUnlock();
			if (parent)
				parent->writeUnlock();

			return success; // success
		} else {
			// only lock leaf node
			node->upgradeToWriteLockOrRestart(versionNode, needRestart);
			if (needRestart)
				goto restart;
			if (parent) {
				parent->readUnlockOrRestart(versionParent, needRestart);
				if (needRestart) {
					node->writeUnlock();
					goto restart;
				}
			}

			success = leaf->insert(k, v, keyComp_);
			node->writeUnlock();
			// node->readUnlockOrRestart(versionParent, needRestart);
			if (success) {
				stats_.num_items++;
			}
			return success; // success
		}
	}

	/**
	 * return v if insert successful
	 * otherwise return an existing value
	 */
	ValueType insert(const KeyType &k, const ValueType &v, bool *result)
	{
		// EBR<UpdateThreshold, Deallocator>::getLocalThreadData().enterCritical();
		// btreeolc::DeferCode c([]() { EBR<UpdateThreshold, Deallocator>::getLocalThreadData().leaveCritical(); });
		int restartCount = 0;
restart:
		// need yield CPU when come here at second time
		if (restartCount++)
			yield(restartCount, true);
		bool needRestart = false;

		// Current node
		NodeBase *node = root_;
		uint64_t versionNode = node->readLockOrRestart(needRestart);
		if (needRestart || (node != root_)) {
			node->readUnlockOrRestart(versionNode, needRestart);
			goto restart;
		}

		// Parent of current node
		BTreeInner *parent = nullptr;
		uint64_t versionParent = 0;

		while (node->getType() == NodeType::BTreeInner) {
			auto inner = static_cast<BTreeInner *>(node);
			// Split eagerly if full
			if (inner->isFull()) {
				// Lock
				if (parent) {
					parent->upgradeToWriteLockOrRestart(versionParent, needRestart);
					if (needRestart)
						goto restart;
				}
				node->upgradeToWriteLockOrRestart(versionNode, needRestart);
				if (needRestart) {
					if (parent)
						parent->writeUnlock();
					goto restart;
				}
				// parent is null and node isn't root
				if (!parent && (node != root_)) {
					// there's a new parent
					node->writeUnlock();
					goto restart;
				}
				// Split
				KeyType sep;
				BTreeInner *newInner = inner->split(sep);
				stats_.inner_nodes++;
				if (parent)
					parent->insert(sep, newInner, keyComp_);
				else
					makeRoot(sep, inner, newInner);

				if (parent) {
					parent->downgradeToReadLock(versionParent);
				}

				if (keyComp_(k, sep) > 0) {
					inner->writeUnlock();
					inner = newInner;
					versionNode = newInner->readLockOrRestart(needRestart);
					if (needRestart)
						goto restart;
				} else {
					node->downgradeToReadLock(versionNode);
				}
			}

			if (parent) {
				parent->readUnlockOrRestart(versionParent, needRestart);
				if (needRestart)
					goto restart;
			}

			parent = inner;
			versionParent = versionNode;

			node = inner->childAt(inner->lowerBound(k, keyComp_));
			// prefetch((char *)node, sizeof(NodeMetaData));
			inner->checkOrRestart(versionNode, needRestart);
			if (needRestart)
				goto restart;

			versionNode = node->readLockOrRestart(needRestart);
			if (needRestart)
				goto restart;
		} // while

		auto leaf = static_cast<BTreeLeaf *>(node);
		ValueType insertRes;
		bool success;

		// Split leaf if full
		if (leaf->getCount() == leaf->maxEntries) {
			// Lock
			if (parent) {
				parent->upgradeToWriteLockOrRestart(versionParent, needRestart);
				if (needRestart) {
					node->readUnlockOrRestart(versionNode, needRestart);
					goto restart;
				}
			}
			node->upgradeToWriteLockOrRestart(versionNode, needRestart);
			if (needRestart) {
				node->readUnlockOrRestart(versionNode, needRestart);
				if (parent)
					parent->writeUnlock();
				goto restart;
			}
			if (!parent && (node != root_)) {
				// there's a new parent
				node->writeUnlock();
				goto restart;
			}
			// Split
			KeyType sep;
			BTreeLeaf *newLeaf = leaf->split(sep);
			stats_.leaf_nodes++;
			if (keyComp_(k, sep) > 0) {
				insertRes = newLeaf->insert(k, v, keyComp_, success);
			} else {
				insertRes = leaf->insert(k, v, keyComp_, success);
			}

			if (parent)
				parent->insert(sep, newLeaf, keyComp_);
			else
				makeRoot(sep, leaf, newLeaf);
			// Unlock and restart
			node->writeUnlock();
			if (parent)
				parent->writeUnlock();

			if (success) {
				stats_.num_items++;
			}
			if (result)
				*result = success;
			return insertRes; // success
		} else {
			// only lock leaf node
			node->upgradeToWriteLockOrRestart(versionNode, needRestart);
			if (needRestart) {
				node->readUnlockOrRestart(versionNode, needRestart);
				goto restart;
			}
			if (parent) {
				parent->readUnlockOrRestart(versionParent, needRestart);
				if (needRestart) {
					node->writeUnlock();
					goto restart;
				}
			}
			insertRes = leaf->insert(k, v, keyComp_, success);
			node->writeUnlock();
			// node->readUnlockOrRestart(versionParent, needRestart);

			if (result)
				*result = success;
			if (success) {
				stats_.num_items++;
			}
			return insertRes; // success
		}
	}

	/**
	 * return v if the <key, value> already exists
	 * otherwise, create a <k, v> and return v
	 */
	ValueType getValue(const KeyType &k, std::function<ValueType(void)> createValue)
	{
		// EBR<UpdateThreshold, Deallocator>::getLocalThreadData().enterCritical();
		// btreeolc::DeferCode c([]() { EBR<UpdateThreshold, Deallocator>::getLocalThreadData().leaveCritical(); });
		int restartCount = 0;
restart:
		// need yield CPU when come here at second time
		if (restartCount++)
			yield(restartCount, true);
		bool needRestart = false;

		// Current node
		NodeBase *node = root_;
		uint64_t versionNode = node->readLockOrRestart(needRestart);
		if (needRestart || (node != root_)) {
			node->readUnlockOrRestart(versionNode, needRestart);
			goto restart;
		}

		// Parent of current node
		BTreeInner *parent = nullptr;
		uint64_t versionParent = 0;

		while (node->getType() == NodeType::BTreeInner) {
			auto inner = static_cast<BTreeInner *>(node);
			// Split eagerly if full
			if (inner->isFull()) {
				// Lock
				if (parent) {
					parent->upgradeToWriteLockOrRestart(versionParent, needRestart);
					if (needRestart)
						goto restart;
				}
				node->upgradeToWriteLockOrRestart(versionNode, needRestart);
				if (needRestart) {
					if (parent)
						parent->writeUnlock();
					goto restart;
				}
				// parent is null and node isn't root
				if (!parent && (node != root_)) {
					// there's a new parent
					node->writeUnlock();
					goto restart;
				}
				// Split
				KeyType sep;
				BTreeInner *newInner = inner->split(sep);
				stats_.inner_nodes++;
				if (parent)
					parent->insert(sep, newInner, keyComp_);
				else
					makeRoot(sep, inner, newInner);

				if (parent) {
					parent->downgradeToReadLock(versionParent);
				}

				if (keyComp_(k, sep) > 0) {
					inner->writeUnlock();
					inner = newInner;
					versionNode = newInner->readLockOrRestart(needRestart);
					if (needRestart)
						goto restart;
				} else {
					node->downgradeToReadLock(versionNode);
				}
			}

			if (parent) {
				parent->readUnlockOrRestart(versionParent, needRestart);
				if (needRestart)
					goto restart;
			}

			parent = inner;
			versionParent = versionNode;

			node = inner->childAt(inner->lowerBound(k, keyComp_));
			// prefetch((char *)node, sizeof(NodeMetaData));
			inner->checkOrRestart(versionNode, needRestart);
			if (needRestart)
				goto restart;

			versionNode = node->readLockOrRestart(needRestart);
			if (needRestart)
				goto restart;
		} // while

		auto leaf = static_cast<BTreeLeaf *>(node);
		ValueType insertRes;
		bool success;

		// Split leaf if full
		if (leaf->getCount() == leaf->maxEntries) {
			// Lock
			if (parent) {
				parent->upgradeToWriteLockOrRestart(versionParent, needRestart);
				if (needRestart) {
					node->readUnlockOrRestart(versionNode, needRestart);
					goto restart;
				}
			}
			node->upgradeToWriteLockOrRestart(versionNode, needRestart);
			if (needRestart) {
				node->readUnlockOrRestart(versionNode, needRestart);
				if (parent) {
					parent->writeUnlock();
				}
				goto restart;
			}
			if (!parent && (node != root_)) {
				// there's a new parent
				node->writeUnlock();
				goto restart;
			}
			// Split
			KeyType sep;
			BTreeLeaf *newLeaf = leaf->split(sep);
			stats_.leaf_nodes++;
			if (keyComp_(k, sep) > 0) {
				insertRes = newLeaf->getValue(k, keyComp_, createValue, success);
			} else {
				insertRes = leaf->getValue(k, keyComp_, createValue, success);
			}

			if (success) {
				stats_.num_items++;
			}
			if (parent)
				parent->insert(sep, newLeaf, keyComp_);
			else
				makeRoot(sep, leaf, newLeaf);
			// Unlock and restart
			node->writeUnlock();
			if (parent)
				parent->writeUnlock();
			return insertRes; // success
		} else {
			// only lock leaf node
			node->upgradeToWriteLockOrRestart(versionNode, needRestart);
			if (needRestart) {
				node->readUnlockOrRestart(versionNode, needRestart);
				goto restart;
			}
			if (parent) {
				parent->readUnlockOrRestart(versionParent, needRestart);
				if (needRestart) {
					node->writeUnlock();
					goto restart;
				}
			}
			insertRes = leaf->getValue(k, keyComp_, createValue, success);
			if (success) {
				stats_.num_items++;
			}
			node->writeUnlock();
			return insertRes; // success
		}
	}

	/**
	 * Delete key and its corresponding value if only if the value satisfies the predicate
	 * return RemoveResult::GOOD if key exists and delete successfully
	 *        RemoveResult::KEY_NOT_FOUND if key does not exists
	 *        RemoveResult::VALUE_NOT_SATISFYING_PREDICATE if the value does not pass value_predicate check
	 */
	btreeolc::RemoveResult remove(const KeyType &key, std::function<btreeolc::RemovePredicateResult(const ValueType &)> value_predicate)
	{
		ValueType v;
		return _remove_with_value_predicate(std::make_pair(key, v), value_predicate);
	}

	/**
	 * Delete key and its corresponding value
	 * return true if key exists and delete successfully
	 */
	bool remove(const KeyType &key)
	{
		return _remove(key);
	}

	/**
	 * Delete <key, value>
	 * return true if <key, value> exists and delete successfully
	 */
	bool remove(const KeyType &key, ValueType value)
	{
		return _remove(key);
	}

	/**
	 * @param leftExist: Left interval is `[` when leftExist is true, otherwise '('
	 *                   Invalid when leftExist is false
	 * @param rightExist: Right interval is `]` when rightExist is true, otherwise ')'
	 *                    Invalid when rightExist is false
	 * @param limit: Upper bound for the number of elements
	 *               unlimited if limit is 0
	 *
	 * @return: return true if need to retry
	 *
	 * NOTE: res will be clear
	 */
	void scan(const KeyType &lowKey, const KeyType &highKey, bool leftExist, bool rightExist, uint32_t limit, std::vector<KeyValuePair> &res)
	{
		// EBR<UpdateThreshold, Deallocator>::getLocalThreadData().enterCritical();
		// btreeolc::DeferCode c([]() { EBR<UpdateThreshold, Deallocator>::getLocalThreadData().leaveCritical(); });
		int restartCount = 0;
restart:
		res.clear();
		if (restartCount++)
			yield(restartCount);
		bool needRestart = false;

		NodeBase *node = root_;
		uint64_t versionNode = node->readLockOrRestart(needRestart);
		if (needRestart || (node != root_)) {
			node->readUnlockOrRestart(versionNode, needRestart);
			goto restart;
		}

		// Parent of current node
		BTreeInner *parent = nullptr;
		uint64_t versionParent;

		// find the first leafNode
		while (node->getType() == NodeType::BTreeInner) {
			auto inner = static_cast<BTreeInner *>(node);

			if (parent) {
				parent->readUnlockOrRestart(versionParent, needRestart);
				if (needRestart)
					goto restart;
			}

			parent = inner;
			versionParent = versionNode;
			node = inner->childAt(inner->lowerBound(lowKey, keyComp_));

			// prefetch((char *)node, kPageSize);
			inner->checkOrRestart(versionNode, needRestart);
			if (needRestart)
				goto restart;
			versionNode = node->readLockOrRestart(needRestart);
			if (needRestart)
				goto restart;
		}
		node->checkOrRestart(versionNode, needRestart);
		if (needRestart)
			goto restart;
		auto leaf = static_cast<BTreeLeaf *>(node);
		unsigned pos = leaf->lowerBound(lowKey, keyComp_);
		if (leaf == nullptr)
			return;
		// Adjust the leaf node
		// according to the `leftExist`
		if (!leftExist && keyComp_(lowKey, leaf->keys_[pos]) == 0) {
			if ((int)pos == leaf->getCount() - 1) {
				leaf = leaf->next_;
				pos = 0;
			} else {
				pos++;
			}
		}
		if (leaf == nullptr)
			return;

		leaf->iteratorEnter(needRestart);
		{
			BPlusTreeIterator itr(leaf, pos);
			if (itr == retryItr())
				goto restart;

			while ((limit == 0 || res.size() < limit) && (itr != endItr()) &&
			       ((!rightExist && keyComp_(itr.key(), highKey) < 0) || (rightExist && keyComp_(itr.key(), highKey) <= 0))) {
				res.push_back({ itr.key(), itr.value() });
				itr++;
				// needs scan again
				if (itr == retryItr())
					goto restart;
			}
		}
	}

	/**
	 * Range scan items starting at `startKey` for update.
	 * Leaves will be write-locked.
	 * The scan ends when processor returns false or all items are traversed.
	 */
	void scanForUpdate(const KeyType &startKey, std::function<bool(const KeyType &, ValueType &, bool)> processor)
	{
		bool leftExist = true;
		// EBR<UpdateThreshold, Deallocator>::getLocalThreadData().enterCritical();
		// btreeolc::DeferCode c([]() { EBR<UpdateThreshold, Deallocator>::getLocalThreadData().leaveCritical(); });
		int restartCount = 0;
		int leavesTraversed = 0;
		KeyType lowKey = startKey;
restart:
		if (restartCount++)
			yield(restartCount);
		bool needRestart = false;

		NodeBase *node = root_;
		uint64_t versionNode = node->readLockOrRestart(needRestart);
		if (needRestart || (node != root_)) {
			node->readUnlockOrRestart(versionNode, needRestart);
			goto restart;
		}

		// Parent of current node
		BTreeInner *parent = nullptr;
		uint64_t versionParent;

		// find the first leafNode
		while (node->getType() == NodeType::BTreeInner) {
			auto inner = static_cast<BTreeInner *>(node);

			if (parent) {
				parent->readUnlockOrRestart(versionParent, needRestart);
				if (needRestart)
					goto restart;
			}

			parent = inner;
			versionParent = versionNode;
			node = inner->childAt(inner->lowerBound(lowKey, keyComp_));

			// prefetch((char *)node, kPageSize);
			inner->checkOrRestart(versionNode, needRestart);
			if (needRestart)
				goto restart;
			versionNode = node->readLockOrRestart(needRestart);
			if (needRestart)
				goto restart;
		}
		auto leaf = static_cast<BTreeLeaf *>(node);

		node->upgradeToWriteLockOrRestart(versionNode, needRestart);
		if (needRestart) {
			if (leavesTraversed == 0) {
				lowKey = startKey;
			}
			goto restart;
		}
		if (parent) {
			parent->readUnlockOrRestart(versionParent, needRestart);
			if (needRestart) {
				node->writeUnlock();
				goto restart;
			}
		}

		unsigned pos = leaf->lowerBound(lowKey, keyComp_);

		bool quit = false;
		BTreeLeaf *nextLeaf = leaf->next_;
		for (unsigned p = pos; p < leaf->getCount(); ++p) {
			bool lastItem = nextLeaf == nullptr && p + 1 == leaf->getCount();
			bool end = processor(leaf->keys_[p], leaf->values_[p], lastItem);
			if (end) {
				quit = true;
				break;
			}
		}
		node->writeUnlock();
		leavesTraversed++;
		if (quit == false && nextLeaf != nullptr) {
			// versionNode = nextLeaf->readLockOrRestart(needRestart);
			if (nextLeaf->getCount() > 0) {
				lowKey = nextLeaf->keys_[0];
			} else {
				quit = true;
			}
			// nextLeaf->readUnlockOrRestart(versionNode, needRestart);
			// goto restart;
		} else {
			quit = true;
		}

		if (quit == false) {
			goto restart;
		}
	}

	/**
	 * Assume the nodes in stack are all optimistically read-locked,
	 * meaning the version numbers are stored in the stack.
	 * 1. If there is a sibling doesn't meet the "half full" criteria,
	 * try to borrow a KeyType from the sibling.
	 * 2. Otherwise, try to merge a sibling, left sibling first.
	 */
	bool EraseMerge(std::vector<StackNodeElement> &stack)
	{
		assert(stack.empty() == false);
		if (stack.size() == 1) {
			// auto e = stack.back();
			// bool needRestart = true;
			// e.node->readUnlockOrRestart(e.version, needRestart);
			return false;
		}
		// the last element of stack is a leaf Node
		StackNodeElement tope = stack[stack.size() - 1];
		NodeBase *childNode = tope.node;
		uint64_t childVersion = tope.version;
		unsigned childPos = tope.pos;
		{
			bool restart = false;
			childNode->checkOrRestart(childVersion, restart);
			if (restart) {
				return false;
			}
		}
		/*           R[1]
		      A[1]           B[0]
		  A1[2] A2[2]      B1[1]
		*/
		assert(stack.size() >= 2);
		StackNodeElement parente = stack[stack.size() - 2];
		// parent node must be inner node
		BTreeInner *parentNode = static_cast<BTreeInner *>(parente.node);
		uint64_t parentVersion = parente.version;
		// unsigned parentPos = parente.pos;

		uint64_t siblingVersion;
		enum class MergeOperation {
			BorrowFromLeft,
			BorrowFromRight,
			LeftToRight,
			RightToLeft, // put right on left
			NoMerge, // no operation
		};

		MergeOperation opt = MergeOperation::NoMerge;
		// the position of left sibling or right sibling
		int siblingPos = -1;
		// If current node is the only node stored in parent
		bool single_child = false;
		auto choose_one_sibling = [&]() -> NodeBase * {
			unsigned leftSiblingPos = childPos - 1;
			unsigned rightSiblingPos = childPos + 1;
			NodeBase *leftSibling = nullptr, *rightSibling = nullptr;
			if (leftSiblingPos >= 0 && leftSiblingPos < parentNode->getCount()) {
				leftSibling = parentNode->childAt(leftSiblingPos);
			}
			if (rightSiblingPos > 0 && rightSiblingPos <= parentNode->getCount()) {
				rightSibling = parentNode->childAt(rightSiblingPos);
			}
			if (leftSibling == nullptr && rightSibling == nullptr) {
				single_child = true;
			}
			// try borrow from left sibling
			if (leftSibling) {
				bool restart = false;
				siblingVersion = leftSibling->readLockOrRestart(restart);
				if (restart == false && getSurplus(leftSibling) > 0) {
					siblingPos = leftSiblingPos;
					opt = MergeOperation::BorrowFromLeft;
					return leftSibling;
				}
			}
			// try borrow from right sibling
			if (rightSibling) {
				bool restart = false;
				siblingVersion = rightSibling->readLockOrRestart(restart);
				if (restart == false && getSurplus(rightSibling) > 0) {
					siblingPos = rightSiblingPos;
					opt = MergeOperation::BorrowFromRight;
					return rightSibling;
				}
			}

			if (leftSibling) {
				bool restart = false;
				// get the read lock
				siblingVersion = leftSibling->readLockOrRestart(restart);
				// if restart is true, try right sibling
				if (restart == false) {
					if (hasEnoughSpace(leftSibling, childNode->getCount())) {
						siblingPos = leftSiblingPos;
						opt = MergeOperation::RightToLeft;
						return leftSibling;
					}
				}
				// release the read lock of left sibling
				// EBR<UpdateThreshold, Deallocator>::getLocalThreadData().leaveCritical();
			}

			if (rightSibling) {
				bool restart = false;
				// get the read lock
				siblingVersion = rightSibling->readLockOrRestart(restart);
				if (restart == false) {
					if (hasEnoughSpace(rightSibling, childNode->getCount())) {
						siblingPos = rightSiblingPos;
						opt = MergeOperation::LeftToRight;
						return rightSibling;
					}
				}
				// EBR<UpdateThreshold, Deallocator>::getLocalThreadData().leaveCritical();
			}
			return nullptr;
		};

		bool restart = false;
		NodeBase *siblingNode = choose_one_sibling();
		if (siblingNode == nullptr) {
			// if childNode is the only child of parent, try merge the parent node first so as to get more "siblings".
			if (single_child && stack.size() > 2) {
				stack.pop_back();
				EraseMerge(stack);
			}
			return false; // retry
		}

		// Lock protocol, top to bottom, left to right

		parentNode->upgradeToWriteLockOrRestart(parentVersion, restart);
		if (restart) {
			return false;
		}

		if (opt == MergeOperation::LeftToRight || opt == MergeOperation::BorrowFromRight) {
			childNode->upgradeToWriteLockOrRestart(childVersion, restart);
			if (restart) {
				parentNode->writeUnlock();
				return false;
			}
			siblingNode->upgradeToWriteLockOrRestart(siblingVersion, restart);
			if (restart) {
				childNode->writeUnlock();
				parentNode->writeUnlock();
				return false;
			}
		} else if (opt == MergeOperation::RightToLeft || opt == MergeOperation::BorrowFromLeft) {
			siblingNode->upgradeToWriteLockOrRestart(siblingVersion, restart);
			if (restart) {
				parentNode->writeUnlock();
				return false;
			}
			childNode->upgradeToWriteLockOrRestart(childVersion, restart);
			if (restart) {
				siblingNode->writeUnlock();
				parentNode->writeUnlock();
				return false;
			}
		}

		bool parentNeedMerge = false, changeRoot = false;
		if (childNode->getType() == NodeType::BTreeLeaf) {
			auto child = static_cast<BTreeLeaf *>(childNode);
			auto sibling = static_cast<BTreeLeaf *>(siblingNode);
			if (opt == MergeOperation::LeftToRight) {
				child->merge(sibling);
				stats_.leaf_nodes--;
				// KeyType siblingMaxKey = sibling->max_key();
				// parentNode->keys_[childPos] = siblingMaxKey;
				changeRoot = parentNode->erase(childPos);
				// if (changeRoot && parentNode == root_.load()) {
				if (changeRoot && parentNode == root_) {
					assert(((uint64_t)parentNode) != 0xffffffffffffffffull);
					EBR<UpdateThreshold, Deallocator>::getLocalThreadData().addRetiredNode(parentNode);
					root_.store(child);
					// if (parentNode == root_) root_.store(child);
				}
			} else if (opt == MergeOperation::RightToLeft) {
				sibling->merge(child);
				stats_.leaf_nodes--;
				// KeyType childMaxKey = child->max_key();
				// parentNode->keys_[siblingPos] = childMaxKey;
				changeRoot = parentNode->erase(siblingPos);
				if (changeRoot && parentNode == root_.load()) {
					// if (changeRoot) {
					assert(((uint64_t)parentNode) != 0xffffffffffffffffull);
					EBR<UpdateThreshold, Deallocator>::getLocalThreadData().addRetiredNode(parentNode);
					root_.store(sibling);
				}
			} else if (opt == MergeOperation::BorrowFromRight) {
				reallocNode(childNode, siblingNode, 1, parentNode, childPos);
			} else if (opt == MergeOperation::BorrowFromLeft) {
				reallocNode(siblingNode, childNode, 0, parentNode, siblingPos);
			}
		} else {
			auto child = static_cast<BTreeInner *>(childNode);
			auto sibling = static_cast<BTreeInner *>(siblingNode);
			if (opt == MergeOperation::LeftToRight) {
				const KeyType &subTreeMaxKey = _getSubTreeMaxKey(child->childAt(child->getCount()));
				child->merge(sibling, subTreeMaxKey);
				stats_.inner_nodes--;
				// KeyType siblingSubTreeMaxKey = _getSubTreeMaxKey(sibling);
				changeRoot = parentNode->erase(childPos);
				if (changeRoot && parentNode == root_.load()) {
					// if (changeRoot) {
					assert(((uint64_t)parentNode) != 0xffffffffffffffffull);
					EBR<UpdateThreshold, Deallocator>::getLocalThreadData().addRetiredNode(parentNode);
					root_.store(child);
				}
			} else if (opt == MergeOperation::RightToLeft) {
				const KeyType &subTreeMaxKey = _getSubTreeMaxKey(sibling->childAt(sibling->getCount()));
				sibling->merge(child, subTreeMaxKey);
				stats_.inner_nodes--;
				changeRoot = parentNode->erase(siblingPos);
				if (changeRoot && parentNode == root_.load()) {
					// if (changeRoot) {
					assert(((uint64_t)parentNode) != 0xffffffffffffffffull);
					EBR<UpdateThreshold, Deallocator>::getLocalThreadData().addRetiredNode(parentNode);
					root_.store(sibling);
				}
			} else if (opt == MergeOperation::BorrowFromRight) {
				reallocNode(childNode, siblingNode, 1, parentNode, childPos);
			} else if (opt == MergeOperation::BorrowFromLeft) {
				reallocNode(siblingNode, childNode, 0, parentNode, siblingPos);
			}
		}

		if (opt != MergeOperation::BorrowFromRight && opt != MergeOperation::BorrowFromLeft) {
			parentNeedMerge = parentNode->needMerge();
		}

		if (opt == MergeOperation::LeftToRight || opt == MergeOperation::BorrowFromRight) {
			siblingNode->writeUnlock();
			childNode->writeUnlock();
		} else if (opt == MergeOperation::RightToLeft || opt == MergeOperation::BorrowFromLeft) {
			childNode->writeUnlock();
			siblingNode->writeUnlock();
		}

		parentNode->writeUnlock();
		if (parentNeedMerge) {
			stack.pop_back();
			parentVersion = parentNode->readLockOrRestart(restart);
			stack.back().version = parentVersion;
			EraseMerge(stack);
		}
		return true;
	}

	/**
	 * find key and all its corresponding values
	 * return true if key exists
	 */
	bool lookup(const KeyType &key, ValueType &result)
	{
		return _lookup(key, result);
	}

	/**
	 * find key and all its corresponding values
	 * return true if key exists
	 */
	bool lookupForUpdate(const KeyType &key, std::function<void(const KeyType &key, ValueType &value)> update_processor)
	{
		return _lookupForUpdate(key, update_processor);
	}

	/**
	 * find <key, value>
	 * return true if <key, value> exists and delete successfully
	 * NOTE: lookup only append data to result
	 */
	bool lookup(const KeyType &key, const ValueType &value, ValueType &result)
	{
		return _lookup({ key, value }, result, false);
	}

	/**
	 * Returns retryItr() iterator for the B+ Tree
	 */
	BPlusTreeIterator retryItr()
	{
		return BPlusTreeIterator::getRetryIterator();
	}

	/**
	 * Returns endItr() iterator for the B+ Tree
	 */
	BPlusTreeIterator endItr()
	{
		return BPlusTreeIterator::getEndIterator();
	}

	uint64_t getInnerNodeSize() const
	{
		return InnerPageSize;
	}

	uint64_t getLeafNodeSize() const
	{
		return sizeof(BTreeLeaf);
	}

	uint64_t getNumInnerNodes() const
	{
		return stats_.inner_nodes.load();
	}

	uint64_t getNumLeafNodes() const
	{
		return stats_.leaf_nodes.load();
	}

	std::size_t size() const
	{
		return stats_.num_items;
	}

	double getAverageFillFactor() const
	{
		auto numLeaves = getNumLeafNodes();
		auto maximumItems = numLeaves * BTreeLeaf::maxEntries;
		return stats_.num_items / (maximumItems * 1.0);
	}

	void EBREnter()
	{
		EBR<UpdateThreshold, Deallocator>::getLocalThreadData().enterCritical();
	}

	void EBRExit()
	{
		EBR<UpdateThreshold, Deallocator>::getLocalThreadData().leaveCritical();
	}

    private:
	const KeyComparator keyComp_;
	const ValueComparator valueComp_;

	/** whether key is unique */
	const int keyUnique_;

	std::atomic<NodeBase *> root_;

	struct tree_stats {
		std::atomic<uint64_t> inner_nodes{ 0 };
		char pad1_[64];
		std::atomic<uint64_t> leaf_nodes{ 0 };
		char pad2_[64];
		std::atomic<uint64_t> num_items{ 0 };
	};
	char pad_[64];
	tree_stats stats_;

	const KeyType &_getSubTreeMaxKey(NodeBase *node)
	{
		int restartCount = 0;
restart:
		if (restartCount++)
			yield(restartCount);

		bool needRestart = false;
		uint64_t version = node->readLockOrRestart(needRestart);
		if (needRestart)
			goto restart;

		BTreeInner *parent = nullptr;
		uint64_t parentVersion = 0;

		while (node->getType() == NodeType::BTreeInner) {
			auto inner = static_cast<BTreeInner *>(node);
			if (parent) {
				parent->readUnlockOrRestart(parentVersion, needRestart);
				if (needRestart)
					goto restart;
			}

			parent = inner;
			parentVersion = version;

			// get the last child
			auto pos = node->getCount();
			node = inner->childAt(pos);

			inner->checkOrRestart(version, needRestart);
			if (needRestart)
				goto restart;
			version = node->readLockOrRestart(needRestart);
			if (needRestart)
				goto restart;
		}

		auto leaf = static_cast<BTreeLeaf *>(node);
		// int pos = leaf->getCount() ? leaf->getCount() - 1 : 0;
		const KeyType &res = leaf->max_key();
		if (parent) {
			parent->readUnlockOrRestart(parentVersion, needRestart);
			if (needRestart) {
				leaf->readUnlockOrRestart(version, needRestart);
				goto restart;
			}
		}

		node->readUnlockOrRestart(version, needRestart);
		if (needRestart)
			goto restart;
		return res;
	}

	btreeolc::RemoveResult _remove_with_value_predicate(const KeyValuePair &element,
							    std::function<btreeolc::RemovePredicateResult(const ValueType &)> predicate)
	{
		// EBR<UpdateThreshold, Deallocator>::getLocalThreadData().enterCritical();
		// btreeolc::DeferCode c([]() { EBR<UpdateThreshold, Deallocator>::getLocalThreadData().leaveCritical(); });
		int restartCount = 0;
		btreeolc::RemoveResult saved_result = btreeolc::RemoveResult::VALUE_NOT_SATISFYING_PREDICATE;
		bool result_saved = false;
restart:
		if (restartCount++)
			yield(restartCount, true);
		bool needRestart = false;
		// stores the path from root to leaf.
		// each element stores the node and the position in the parent node
		// from which the current node is derived.
		std::vector<StackNodeElement> stack;
		NodeBase *node = root_.load();
		uint64_t versionNode = node->readLockOrRestart(needRestart);
		if (needRestart || node != root_) {
			node->readUnlockOrRestart(versionNode, needRestart);
			goto restart;
		}
		stack.push_back(StackNodeElement{ node, -1, versionNode });

		// Parent of current node
		BTreeInner *parent = nullptr;
		uint64_t versionParent = 0;

		while (node->getType() == NodeType::BTreeInner) {
			BTreeInner *inner = static_cast<BTreeInner *>(node);
			if (parent) {
				// check the version only, don't call `readUnlockOrRestart`
				parent->checkOrRestart(versionParent, needRestart);
				if (needRestart) {
					goto restart;
				}
			}
			// inner -> parent, inner -> parent.child
			parent = inner;
			versionParent = versionNode;

			unsigned pos = inner->lowerBound(element.first, keyComp_);
			node = inner->childAt(pos);
			inner->checkOrRestart(versionNode, needRestart);
			if (needRestart) {
				goto restart;
			}
			versionNode = node->readLockOrRestart(needRestart);
			if (needRestart) {
				goto restart;
			}

			stack.push_back(StackNodeElement{ node, int(pos), versionNode });
		}

		if (parent) {
			// check the version only, don't call `readUnlockOrRestart`
			parent->checkOrRestart(versionParent, needRestart);
			if (needRestart) {
				node->readUnlockOrRestart(versionNode, needRestart);
				assert(needRestart == false);
				goto restart;
			}
		}
		// reach the leaf node
		BTreeLeaf *leafNode = static_cast<BTreeLeaf *>(node);
		unsigned pos = leafNode->lowerBound(element.first, keyComp_);
		btreeolc::RemoveResult result = btreeolc::RemoveResult::GOOD;
		bool leafNeedMerge = false;
		if (pos < leafNode->getCount() && result_saved == false) {
			const KeyType &key = leafNode->keys_[pos];
			const ValueType &value = leafNode->values_[pos];
			// const KeyValuePair & kv = ;
			//  the leaf node contains the key
			if (keyComp_(key, element.first) == 0) {
				leafNode->upgradeToWriteLockOrRestart(versionNode, needRestart);
				if (needRestart) {
					leafNode->readUnlockOrRestart(versionNode, needRestart);
					goto restart;
				}
				if (parent) {
					// check the version only, don't call `readUnlockOrRestart`
					parent->checkOrRestart(versionParent, needRestart);
					if (needRestart) {
						leafNode->writeUnlock();
						goto restart;
					}
				}
				auto predicate_result = predicate(value);

				if (predicate_result == btreeolc::RemovePredicateResult::GOOD) {
					leafNode->erase(pos, keyComp_, valueComp_, false);
					result = btreeolc::RemoveResult::GOOD;
					stats_.num_items--;
				} else if (predicate_result == btreeolc::RemovePredicateResult::VALUE_HAS_OTHER_REFERENCE) {
					result = btreeolc::RemoveResult::VALUE_HAS_OTHER_REFERENCE;
				} else {
					result = btreeolc::RemoveResult::VALUE_NOT_SATISFYING_PREDICATE;
				}

				leafNode->downgradeToReadLock(versionNode);

				result_saved = true;
				saved_result = result;

				leafNeedMerge = leafNode->needMerge();

				// No merge if leaf is the root.
				if (stack.size() > 1 && leafNeedMerge) {
					stack.back().version = versionNode;
					auto stack_copy = stack;
					if (!EraseMerge(stack_copy)) {
						leafNode->readUnlockOrRestart(versionNode, needRestart);
						// The leaf is under-utilize and EraseMerge failed due to conflicts, retry until it succeeds.
						// We save the result of delete operation to avoid deleting a key twice.
						goto restart;
					}
				} else {
					leafNode->readUnlockOrRestart(versionNode, needRestart);
				}
				return result;
			} else {
				result = btreeolc::RemoveResult::KEY_NOT_FOUND;
			}
			result_saved = true;
			saved_result = result;
		} else {
			result = btreeolc::RemoveResult::KEY_NOT_FOUND;
		}

		if (result_saved == false) {
			result_saved = true;
			saved_result = result;
		}

		// Check if the leaf is under-utilized.
		leafNeedMerge = leafNode->needMerge();
		// No merge if leaf is the root.
		if (stack.size() > 1 && leafNeedMerge) {
			stack.back().version = versionNode;
			auto stack_copy = stack;
			if (!EraseMerge(stack_copy)) {
				leafNode->readUnlockOrRestart(versionNode, needRestart);
				// The leaf is under-utilize and EraseMerge failed due to conflict, retry until it succeeds.
				goto restart;
			}
		} else {
			leafNode->readUnlockOrRestart(versionNode, needRestart);
		}

		assert(result_saved);
		return saved_result;
	}

	/**
	 * @param flag Delete key and all its corresponding values when flag is true
	 */
	bool _remove(const KeyType &deleteKey)
	{
		// EBR<UpdateThreshold, Deallocator>::getLocalThreadData().enterCritical();
		// btreeolc::DeferCode c([]() { EBR<UpdateThreshold, Deallocator>::getLocalThreadData().leaveCritical(); });
		int restartCount = 0;
		bool saved_success = false;
		bool result_saved = false;
restart:
		if (restartCount++)
			yield(restartCount, true);
		bool needRestart = false;
		// stores the path from root to leaf.
		// each element stores the node and the position in the parent node
		// from which the current node is derived.
		std::vector<StackNodeElement> stack;
		NodeBase *node = root_.load();
		uint64_t versionNode = node->readLockOrRestart(needRestart);
		if (needRestart || node != root_) {
			node->readUnlockOrRestart(versionNode, needRestart);
			goto restart;
		}
		stack.push_back(StackNodeElement{ node, -1, versionNode });

		// Parent of current node
		BTreeInner *parent = nullptr;
		uint64_t versionParent = 0;

		while (node->getType() == NodeType::BTreeInner) {
			BTreeInner *inner = static_cast<BTreeInner *>(node);
			if (parent) {
				// check the version only, don't call `readUnlockOrRestart`
				parent->checkOrRestart(versionParent, needRestart);
				if (needRestart) {
					goto restart;
				}
			}
			// inner -> parent, inner -> parent.child
			parent = inner;
			versionParent = versionNode;

			unsigned pos = inner->lowerBound(deleteKey, keyComp_);
			node = inner->childAt(pos);
			inner->checkOrRestart(versionNode, needRestart);
			if (needRestart) {
				goto restart;
			}
			versionNode = node->readLockOrRestart(needRestart);
			if (needRestart) {
				goto restart;
			}

			stack.push_back(StackNodeElement{ node, int(pos), versionNode });
		}

		// reach the leaf node
		BTreeLeaf *leafNode = static_cast<BTreeLeaf *>(node);
		leafNode->upgradeToWriteLockOrRestart(versionNode, needRestart);
		if (needRestart) {
			goto restart;
		}
		if (parent) {
			// check the version only, don't call `readUnlockOrRestart`
			parent->checkOrRestart(versionParent, needRestart);
			if (needRestart) {
				leafNode->writeUnlock();
				goto restart;
			}
		}

		unsigned pos = leafNode->lowerBound(deleteKey, keyComp_);
		bool success = false, leafNeedMerge = false;
		if (pos < leafNode->getCount() && saved_success == false) {
			// const KeyValuePair &kv = leafNode->data_[pos];
			const KeyType &key = leafNode->keys_[pos];
			// const ValueType & value = leafNode->values_[pos];
			//  the leaf node contains the key
			if (keyComp_(key, deleteKey) == 0) {
				assert(keyComp_(leafNode->keys_[pos], deleteKey) == 0);
				success = leafNode->erase(pos, keyComp_, valueComp_, false);
				assert(success);
				if (success) {
					stats_.num_items--;
				}
				leafNeedMerge = leafNode->needMerge();

				saved_success = success;
				result_saved = true;
				// // No merge if leaf is the root.
				// if (stack.size() > 1 && leafNeedMerge) {
				//     stack.back().version = versionNode;
				//     auto stack_copy = stack;
				//     if (!EraseMerge(stack_copy)) {
				//         leafNode->readUnlockOrRestart(versionNode, needRestart);
				//         // The leaf is under-utilize and EraseMerge failed due to conflicts, retry until it succeeds.
				//         // We save the result of delete operation to avoid deleting a key twice.
				//         //goto restart;
				//         return saved_success;
				//     }
				// } else {
				//     leafNode->readUnlockOrRestart(versionNode, needRestart);
				// }
				// return success;
			}

			// Could not find the key.
			// However, we still need to check for under-utilizations.
			result_saved = true;
			saved_success = success;
		}

		if (result_saved == false) {
			result_saved = true;
			saved_success = success;
		}

		// Check if the leaf is under-utilized.
		leafNeedMerge = leafNode->needMerge();
		leafNode->writeUnlock();
		// No merge if leaf is the root.
		if (stack.size() > 1 && leafNeedMerge) {
			versionNode = leafNode->readLockOrRestart(needRestart);
			if (needRestart) {
				assert(result_saved);
				assert(saved_success);
				return saved_success;
			}
			stack.back().version = versionNode;
			auto stack_copy = stack;
			if (!EraseMerge(stack_copy)) {
				// The leaf is under-utilize and EraseMerge failed due to conflict, retry until it succeeds.
				// goto restart;
				return saved_success;
			}
		}

		assert(result_saved);
		assert(saved_success);
		return saved_success;
	}

	bool _lookupForUpdate(const KeyType &key, std::function<void(const KeyType &key, ValueType &value)> update_processor)
	{
		// EBR<UpdateThreshold, Deallocator>::getLocalThreadData().enterCritical();
		// btreeolc::DeferCode c([]() { EBR<UpdateThreshold, Deallocator>::getLocalThreadData().leaveCritical(); });
		int restartCount = 0;
restart:
		if (restartCount++)
			yield(restartCount);
		bool needRestart = false;

		NodeBase *node = root_;
		uint64_t versionNode = node->readLockOrRestart(needRestart);
		if (needRestart || (node != root_)) {
			node->readUnlockOrRestart(versionNode, needRestart);
			goto restart;
		}

		// Parent of current node
		BTreeInner *parent = nullptr;
		uint64_t versionParent = 0;

		while (node->getType() == NodeType::BTreeInner) {
			auto inner = static_cast<BTreeInner *>(node);

			if (parent) {
				parent->readUnlockOrRestart(versionParent, needRestart);
				if (needRestart)
					goto restart;
			}

			parent = inner;
			versionParent = versionNode;

			node = inner->childAt(inner->lowerBound(key, keyComp_));
			prefetch((char *)node, sizeof(NodeMetaData));
			inner->checkOrRestart(versionNode, needRestart);
			if (needRestart)
				goto restart;
			versionNode = node->readLockOrRestart(needRestart);
			if (needRestart)
				goto restart;
		}
		node->checkOrRestart(versionNode, needRestart);
		if (needRestart)
			goto restart;

		auto leaf = static_cast<BTreeLeaf *>(node);
		leaf->upgradeToWriteLockOrRestart(versionNode, needRestart);
		if (needRestart) {
			goto restart;
		}
		if (parent) {
			// check the version only, don't call `readUnlockOrRestart`
			parent->checkOrRestart(versionParent, needRestart);
			if (needRestart) {
				leaf->writeUnlock();
				goto restart;
			}
		}

		unsigned pos = leaf->lowerBound(key, keyComp_);

		bool success = false;
		if ((pos < leaf->getCount()) && keyComp_(leaf->keys_[pos], key) == 0) {
			success = true;
			update_processor(leaf->keys_[pos], leaf->values_[pos]);
		}

		leaf->writeUnlock();
		return success;
	}

	/**
	 * @param flag find key and it corresponding value when flag is true
	 */
	bool _lookup(const KeyValuePair &element, ValueType &result, bool flag)
	{
		// EBR<UpdateThreshold, Deallocator>::getLocalThreadData().enterCritical();
		// btreeolc::DeferCode c([]() { EBR<UpdateThreshold, Deallocator>::getLocalThreadData().leaveCritical(); });
		int restartCount = 0;
restart:
		if (restartCount++)
			yield(restartCount);
		bool needRestart = false;

		NodeBase *node = root_;
		uint64_t versionNode = node->readLockOrRestart(needRestart);
		if (needRestart || (node != root_)) {
			node->readUnlockOrRestart(versionNode, needRestart);
			goto restart;
		}

		// Parent of current node
		BTreeInner *parent = nullptr;
		uint64_t versionParent = 0;

		while (node->getType() == NodeType::BTreeInner) {
			auto inner = static_cast<BTreeInner *>(node);

			if (parent) {
				parent->readUnlockOrRestart(versionParent, needRestart);
				if (needRestart)
					goto restart;
			}

			parent = inner;
			versionParent = versionNode;

			node = inner->childAt(inner->lowerBound(element.first, keyComp_));
			prefetch((char *)node, sizeof(NodeMetaData));
			inner->checkOrRestart(versionNode, needRestart);
			if (needRestart)
				goto restart;
			versionNode = node->readLockOrRestart(needRestart);
			if (needRestart)
				goto restart;
		}
		auto leaf = static_cast<BTreeLeaf *>(node);
		unsigned pos = leaf->lowerBound(element.first, keyComp_);
		bool success = false;
		if ((pos < leaf->getCount()) && keyComp_(leaf->keys_[pos], element.first) == 0) {
			if (flag) {
				success = true;
				result = leaf->values_[pos];
			} else {
				if (valueComp_(element.second, leaf->values_[pos]) == 0) {
					success = true;
					result = leaf->values_[pos];
				}
			}
		}
		if (parent) {
			parent->readUnlockOrRestart(versionParent, needRestart);
			if (needRestart) {
				goto restart;
			}
		}
		node->readUnlockOrRestart(versionNode, needRestart);
		if (needRestart)
			goto restart;

		return success;
	}

	/**
	 * find key and it's corresponding value
	 */
	bool _lookup(const KeyType &key, ValueType &result)
	{
		int restartCount = 0;
restart:
		if (restartCount++)
			yield(restartCount);
		bool needRestart = false;

		NodeBase *node = root_;
		uint64_t versionNode = node->readLockOrRestart(needRestart);
		if (needRestart || (node != root_)) {
			node->readUnlockOrRestart(versionNode, needRestart);
			goto restart;
		}

		// Parent of current node
		BTreeInner *parent = nullptr;
		uint64_t versionParent = 0;

		while (node->getType() == NodeType::BTreeInner) {
			auto inner = static_cast<BTreeInner *>(node);

			if (parent) {
				parent->readUnlockOrRestart(versionParent, needRestart);
				if (needRestart)
					goto restart;
			}

			parent = inner;
			versionParent = versionNode;

			node = inner->childAt(inner->lowerBound(key, keyComp_));
			prefetch((char *)node, sizeof(NodeMetaData));
			inner->checkOrRestart(versionNode, needRestart);
			if (needRestart)
				goto restart;
			versionNode = node->readLockOrRestart(needRestart);
			if (needRestart)
				goto restart;
		}
		node->checkOrRestart(versionNode, needRestart);
		if (needRestart)
			goto restart;
		auto leaf = static_cast<BTreeLeaf *>(node);
		unsigned pos = leaf->lowerBound(key, keyComp_);
		bool success = false;
		if ((pos < leaf->getCount()) && keyComp_(leaf->keys_[pos], key) == 0) {
			success = true;
			result = leaf->values_[pos];
		}
		if (parent) {
			parent->readUnlockOrRestart(versionParent, needRestart);
			if (needRestart) {
				leaf->readUnlockOrRestart(versionNode, needRestart);
				goto restart;
			}
		}
		node->readUnlockOrRestart(versionNode, needRestart);
		if (needRestart)
			goto restart;
		return success;
	}
};

}
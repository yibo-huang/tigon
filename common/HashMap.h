//
// Created by Yi Lu on 7/14/18.
//

#pragma once

#include "SpinLock.h"
#include <atomic>
#include <glog/logging.h>
#include <unordered_map>

namespace star
{

template <std::size_t N, class KeyType, class ValueType> class HashMap {
    public:
	using hasher = typename std::unordered_map<KeyType, ValueType>::hasher;

	bool remove(const KeyType &key)
	{
		return _applyAt(
			[&key](std::unordered_map<KeyType, ValueType> &map) {
				auto it = map.find(key);
				if (it == map.end()) {
					return false;
				} else {
					map.erase(it);
					return true;
				}
			},
			bucketNo(key));
	}

	bool contains(const KeyType &key)
	{
		return _applyAt([&key](const std::unordered_map<KeyType, ValueType> &map) { return map.find(key) != map.end(); }, bucketNo(key));
	}

	bool insert(const KeyType &key, const ValueType &value)
	{
		return _applyAt(
			[&key, &value](std::unordered_map<KeyType, ValueType> &map) {
				if (map.find(key) != map.end()) {
					return false;
				}
				map[key] = value;
				return true;
			},
			bucketNo(key));
	}

	ValueType &operator[](const KeyType &key)
	{
		return _applyAtRef([&key](std::unordered_map<KeyType, ValueType> &map) -> ValueType & { return map[key]; }, bucketNo(key));
	}

	std::size_t size()
	{
		return _fold(0, [](std::size_t totalSize, const std::unordered_map<KeyType, ValueType> &map) { return totalSize + map.size(); });
	}

	void clear()
	{
		_map([](std::unordered_map<KeyType, ValueType> &map) { map.clear(); });
	}

	void iterate_non_const(std::function<void(const KeyType &, ValueType &)> processor, std::function<void()> unlock_processor)
	{
		std::vector<std::size_t> bucket_counts(N);
		std::size_t max_bucket_count = 0;
		for (std::size_t i = 0; i < N; ++i) {
			// locks_[i].lock();
			std::size_t bucket_count = maps_[i].bucket_count();
			// locks_[i].unlock();
			bucket_counts[i] = bucket_count;
			max_bucket_count = std::max(max_bucket_count, bucket_count);
		}

		for (std::size_t j = 0; j < max_bucket_count; ++j) {
			for (std::size_t i = 0; i < N; ++i) {
				if (j >= bucket_counts[i])
					continue;
				// locks_[i].lock();
				auto bucket_idx = j;
				auto bucket_end = maps_[i].end(bucket_idx);
				for (auto it = maps_[i].begin(bucket_idx); it != bucket_end; ++it) {
					processor(it->first, it->second);
				}
				// locks_[i].unlock();
				unlock_processor();
			}
			unlock_processor();
		}
	}

	void iterate(std::function<void(const KeyType &, const ValueType &)> processor, std::function<void()> unlock_processor)
	{
		std::vector<std::size_t> bucket_counts(N);
		std::size_t max_bucket_count = 0;
		for (std::size_t i = 0; i < N; ++i) {
			// locks_[i].lock();
			std::size_t bucket_count = maps_[i].bucket_count();
			// locks_[i].unlock();
			bucket_counts[i] = bucket_count;
			max_bucket_count = std::max(max_bucket_count, bucket_count);
		}

		for (std::size_t j = 0; j < max_bucket_count; ++j) {
			for (std::size_t i = 0; i < N; ++i) {
				if (j >= bucket_counts[i])
					continue;
				// locks_[i].lock();
				auto bucket_idx = j;
				auto bucket_end = maps_[i].cend(bucket_idx);
				for (auto it = maps_[i].cbegin(bucket_idx); it != bucket_end; ++it) {
					processor(it->first, it->second);
				}
				// locks_[i].unlock();
				unlock_processor();
			}
			unlock_processor();
		}
	}

    private:
	template <class ApplyFunc> auto &_applyAtRef(ApplyFunc applyFunc, std::size_t i)
	{
		DCHECK(i < N) << "index " << i << " is greater than " << N;
		locks_[i].lock();
		auto &result = applyFunc(maps_[i]);
		locks_[i].unlock();
		return result;
	}

	template <class ApplyFunc> auto _applyAt(ApplyFunc applyFunc, std::size_t i)
	{
		DCHECK(i < N) << "index " << i << " is greater than " << N;
		locks_[i].lock();
		auto result = applyFunc(maps_[i]);
		locks_[i].unlock();
		return result;
	}

	template <class MapFunc> void _map(MapFunc mapFunc)
	{
		for (auto i = 0u; i < N; i++) {
			locks_[i].lock();
			mapFunc(maps_[i]);
			locks_[i].unlock();
		}
	}

	template <class T, class FoldFunc> auto _fold(const T &firstValue, FoldFunc foldFunc)
	{
		T finalValue = firstValue;
		for (auto i = 0u; i < N; i++) {
			locks_[i].lock();
			finalValue = foldFunc(finalValue, maps_[i]);
			locks_[i].unlock();
		}
		return finalValue;
	}

	auto bucketNo(const KeyType &key)
	{
		return hasher_(key) % N;
	}

    private:
	hasher hasher_;
	std::unordered_map<KeyType, ValueType> maps_[N];
	SpinLock locks_[N];
};

template <class KeyType, class ValueType> class UnsafeHashMap {
    public:
	bool remove(const KeyType &key)
	{
		auto it = map.find(key);
		if (it == map.end()) {
			return false;
		} else {
			map.erase(it);
			return true;
		}
	}

	ValueType *search(const KeyType &key)
	{
		auto it = map.find(key);
		if (it == map.end()) {
			return nullptr;
		}
		return &it->second;
	}

	bool contains(const KeyType &key)
	{
		return map.find(key) != map.end();
	}

	bool insert(const KeyType &key, const ValueType &value)
	{
		if (map.find(key) != map.end()) {
			return false;
		}
		map[key] = value;
		return true;
	}

	ValueType &operator[](const KeyType &key)
	{
		return map[key];
	}

	std::size_t size()
	{
		return map.size();
	}

	void clear()
	{
		map.clear();
	}

	void iterate(std::function<void(const KeyType &, const ValueType &)> processor)
	{
		CHECK(false);
	}

    private:
	std::unordered_map<KeyType, ValueType> map;
};

} // namespace star

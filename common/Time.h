//
// Created by Yi Lu on 7/22/18.
//

#pragma once

#include <chrono>
#include <functional>
namespace star
{

class Time {
    public:
	static uint64_t now()
	{
		auto now = std::chrono::steady_clock::now();
		return std::chrono::duration_cast<std::chrono::nanoseconds>(now - startTime).count();
	}

	static std::chrono::steady_clock::time_point startTime;
};

class ScopedTimer {
    public:
	ScopedTimer(std::function<void(uint64_t)> f)
		: call_on_destructor(f)
	{
		startTime = std::chrono::steady_clock::now();
	}

	void reset()
	{
		startTime = std::chrono::steady_clock::now();
		ended = false;
	}
	void end()
	{
		auto us = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - startTime).count();
		call_on_destructor(us);
		ended = true;
	}

	~ScopedTimer()
	{
		if (!ended) {
			end();
		}
	}
	bool ended = false;
	std::chrono::steady_clock::time_point startTime;
	std::function<void(uint64_t)> call_on_destructor;
};

} // namespace star

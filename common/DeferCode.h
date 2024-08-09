//
// Created by Xinjing Zhou on 12/11/21.
//

#pragma once

#include <functional>

namespace star
{

class DeferCode {
    public:
	DeferCode(std::function<void()> f_)
		: f(f_)
	{
	}
	~DeferCode()
	{
		f();
	}

    private:
	std::function<void()> f;
};

} // namespace star
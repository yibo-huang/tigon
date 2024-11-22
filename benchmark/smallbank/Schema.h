//
// Created by Yi Lu on 7/15/18.
//

#pragma once

#include "common/ClassOf.h"
#include "common/FixedString.h"
#include "common/Hash.h"
#include "common/Serialization.h"
#include "core/SchemaDef.h"

namespace star
{
namespace smallbank
{
static constexpr auto __BASE_COUNTER__ = __COUNTER__ + 1;
} // namespace smallbank
} // namespace star

#undef NAMESPACE_FIELDS
#define NAMESPACE_FIELDS(x) x(star) x(smallbank)

#define NUM_ACCOUNTS 10000000
#define NUM_HOT_ACCOUNTS 400000

#define SAVINGS_KEY_FIELDS(x, y) x(uint64_t, ACCOUNT_ID)
#define SAVINGS_VALUE_FIELDS(x, y) x(float, BALANCE)

#define SAVINGS_GET_PLAIN_KEY_FUNC              \
        uint64_t get_plain_key() const          \
        {                                       \
                return ACCOUNT_ID;              \
        }

DO_STRUCT(savings, SAVINGS_KEY_FIELDS, SAVINGS_VALUE_FIELDS, NAMESPACE_FIELDS, SAVINGS_GET_PLAIN_KEY_FUNC)

#define CHECKING_KEY_FIELDS(x, y) x(uint64_t, ACCOUNT_ID)
#define CHECKING_VALUE_FIELDS(x, y) x(float, BALANCE)

#define CHECKING_GET_PLAIN_KEY_FUNC             \
        uint64_t get_plain_key() const          \
        {                                       \
                return ACCOUNT_ID;              \
        }

DO_STRUCT(checking, CHECKING_KEY_FIELDS, CHECKING_VALUE_FIELDS, NAMESPACE_FIELDS, CHECKING_GET_PLAIN_KEY_FUNC)

namespace star
{

template <> class Serializer<smallbank::savings::value> {
    public:
	std::string operator()(const smallbank::savings::value &v)
	{
		return Serializer<decltype(v.BALANCE)>()(v.BALANCE);
        }
};

template <> class Deserializer<smallbank::savings::value> {
    public:
	std::size_t operator()(StringPiece str, smallbank::savings::value &result) const
	{
		std::size_t sz = Deserializer<decltype(result.BALANCE)>()(str, result.BALANCE);
		str.remove_prefix(sz);
		return sz;
	}
};

template <> class ClassOf<smallbank::savings::value> {
    public:
	static constexpr std::size_t size()
	{
		return ClassOf<decltype(smallbank::savings::value::BALANCE)>::size();
	}
};

template <> class Serializer<smallbank::checking::value> {
    public:
	std::string operator()(const smallbank::checking::value &v)
	{
		return Serializer<decltype(v.BALANCE)>()(v.BALANCE);
        }
};

template <> class Deserializer<smallbank::checking::value> {
    public:
	std::size_t operator()(StringPiece str, smallbank::checking::value &result) const
	{
		std::size_t sz = Deserializer<decltype(result.BALANCE)>()(str, result.BALANCE);
		str.remove_prefix(sz);
		return sz;
	}
};

template <> class ClassOf<smallbank::checking::value> {
    public:
	static constexpr std::size_t size()
	{
		return ClassOf<decltype(smallbank::checking::value::BALANCE)>::size();
	}
};

} // namespace star
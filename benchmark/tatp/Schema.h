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
namespace tatp
{
static constexpr auto __BASE_COUNTER__ = __COUNTER__ + 1;
} // namespace tatp
} // namespace star

#undef NAMESPACE_FIELDS
#define NAMESPACE_FIELDS(x) x(star) x(tatp)

#define SUBSCRIBER_SUB_NBR 15
#define SUBSCRIBER_BITS_SIZE 2
#define SUBSCRIBER_HEXES_SIZE 10
#define SUBSCRIBER_BYTES_SIZE 10

#define SUBSCRIBER_KEY_FIELDS(x, y) x(uint32_t, S_ID)
#define SUBSCRIBER_VALUE_FIELDS(x, y) x(FixedString<SUBSCRIBER_SUB_NBR>, SUB_NBR) y(FixedString<SUBSCRIBER_BITS_SIZE>, BITS) y(FixedString<SUBSCRIBER_HEXES_SIZE>, HEXES) y(FixedString<SUBSCRIBER_BYTES_SIZE>, BYTES) y(uint32_t, MSC_LOCATION) y(uint32_t, VLR_LOCATION)

#define SUBSCRIBER_GET_PLAIN_KEY_FUNC           \
        uint64_t get_plain_key() const          \
        {                                       \
                return S_ID;                    \
        }

DO_STRUCT(subscriber, SUBSCRIBER_KEY_FIELDS, SUBSCRIBER_VALUE_FIELDS, NAMESPACE_FIELDS, SUBSCRIBER_GET_PLAIN_KEY_FUNC)

#define SEC_SUBSCRIBER_KEY_FIELDS(x, y) x(FixedString<SUBSCRIBER_SUB_NBR>, SUB_NBR)
#define SEC_SUBSCRIBER_VALUE_FIELDS(x, y) x(uint32_t, S_ID)

#define SEC_SUBSCRIBER_GET_PLAIN_KEY_FUNC               \
        uint64_t get_plain_key() const                  \
        {                                               \
                return std::stoi(SUB_NBR.c_str());      \
        }

DO_STRUCT(sec_subscriber, SEC_SUBSCRIBER_KEY_FIELDS, SEC_SUBSCRIBER_VALUE_FIELDS, NAMESPACE_FIELDS, SEC_SUBSCRIBER_GET_PLAIN_KEY_FUNC)

#define ACCESS_INFO_DATA_3_SIZE 3
#define ACCESS_INFO_DATA_4_SIZE 5

#define ACCESS_INFO_KEY_FIELDS(x, y) x(uint32_t, S_ID) y(uint8_t, AI_TYPE)
#define ACCESS_INFO_VALUE_FIELDS(x, y) x(uint8_t, DATA_1) y(uint8_t, DATA_2) y(FixedString<ACCESS_INFO_DATA_3_SIZE>, DATA_3) y(FixedString<ACCESS_INFO_DATA_4_SIZE>, DATA_4)

#define ACCESS_INFO_GET_PLAIN_KEY_FUNC                  \
        uint64_t get_plain_key() const                  \
        {                                               \
                return S_ID * UINT32_MAX + AI_TYPE;     \
        }

DO_STRUCT(access_info, ACCESS_INFO_KEY_FIELDS, ACCESS_INFO_VALUE_FIELDS, NAMESPACE_FIELDS, ACCESS_INFO_GET_PLAIN_KEY_FUNC)

namespace star
{

template <> class Serializer<tatp::subscriber::value> {
    public:
	std::string operator()(const tatp::subscriber::value &v)
	{
		return Serializer<decltype(v.SUB_NBR)>()(v.SUB_NBR) + Serializer<decltype(v.BITS)>()(v.BITS) + Serializer<decltype(v.HEXES)>()(v.HEXES) +
                        Serializer<decltype(v.BYTES)>()(v.BYTES) + Serializer<decltype(v.MSC_LOCATION)>()(v.MSC_LOCATION) + Serializer<decltype(v.VLR_LOCATION)>()(v.VLR_LOCATION);
        }
};

template <> class Deserializer<tatp::subscriber::value> {
    public:
	std::size_t operator()(StringPiece str, tatp::subscriber::value &result) const
	{
		std::size_t sub_nbr_sz = Deserializer<decltype(result.SUB_NBR)>()(str, result.SUB_NBR);
		str.remove_prefix(sub_nbr_sz);
                std::size_t bits_sz = Deserializer<decltype(result.BITS)>()(str, result.BITS);
		str.remove_prefix(bits_sz);
                std::size_t hexes_sz = Deserializer<decltype(result.HEXES)>()(str, result.HEXES);
		str.remove_prefix(hexes_sz);
                std::size_t bytes_sz = Deserializer<decltype(result.BYTES)>()(str, result.BYTES);
		str.remove_prefix(bytes_sz);
                std::size_t msc_location_sz = Deserializer<decltype(result.MSC_LOCATION)>()(str, result.MSC_LOCATION);
		str.remove_prefix(msc_location_sz);
                std::size_t vlr_location_sz = Deserializer<decltype(result.VLR_LOCATION)>()(str, result.VLR_LOCATION);
		str.remove_prefix(vlr_location_sz);

                return sub_nbr_sz + bits_sz + hexes_sz + bytes_sz + msc_location_sz + vlr_location_sz;
	}
};

template <> class ClassOf<tatp::subscriber::value> {
    public:
	static constexpr std::size_t size()
	{
		return ClassOf<decltype(tatp::subscriber::value::SUB_NBR)>::size() + ClassOf<decltype(tatp::subscriber::value::BITS)>::size() +
                        ClassOf<decltype(tatp::subscriber::value::HEXES)>::size() + ClassOf<decltype(tatp::subscriber::value::BYTES)>::size() +
                        ClassOf<decltype(tatp::subscriber::value::MSC_LOCATION)>::size() + ClassOf<decltype(tatp::subscriber::value::VLR_LOCATION)>::size();
	}
};

template <> class Serializer<tatp::access_info::value> {
    public:
	std::string operator()(const tatp::access_info::value &v)
	{
		return Serializer<decltype(v.DATA_1)>()(v.DATA_1) + Serializer<decltype(v.DATA_2)>()(v.DATA_2) +
                        Serializer<decltype(v.DATA_3)>()(v.DATA_3) + Serializer<decltype(v.DATA_4)>()(v.DATA_4);
        }
};

template <> class Deserializer<tatp::access_info::value> {
    public:
	std::size_t operator()(StringPiece str, tatp::access_info::value &result) const
	{
                std::size_t data_1_sz = Deserializer<decltype(result.DATA_1)>()(str, result.DATA_1);
		str.remove_prefix(data_1_sz);
                std::size_t data_2_sz = Deserializer<decltype(result.DATA_1)>()(str, result.DATA_1);
		str.remove_prefix(data_2_sz);
                std::size_t data_3_sz = Deserializer<decltype(result.DATA_1)>()(str, result.DATA_1);
		str.remove_prefix(data_3_sz);
                std::size_t data_4_sz = Deserializer<decltype(result.DATA_1)>()(str, result.DATA_1);
		str.remove_prefix(data_4_sz);

                return data_1_sz + data_2_sz + data_3_sz + data_4_sz;
	}
};

template <> class ClassOf<tatp::access_info::value> {
    public:
	static constexpr std::size_t size()
	{
		return ClassOf<decltype(tatp::access_info::value::DATA_1)>::size() + ClassOf<decltype(tatp::access_info::value::DATA_2)>::size() +
                        ClassOf<decltype(tatp::access_info::value::DATA_3)>::size() + ClassOf<decltype(tatp::access_info::value::DATA_4)>::size();
	}
};

} // namespace star

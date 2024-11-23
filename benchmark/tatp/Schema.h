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

#define SUBSCRIBER_BITS_SIZE 2
#define SUBSCRIBER_HEXES_SIZE 10
#define SUBSCRIBER_BYTES_SIZE 10

#define SUBSCRIBER_KEY_FIELDS(x, y) x(uint32_t, S_ID)
#define SUBSCRIBER_VALUE_FIELDS(x, y) x(FixedString<15>, SUB_NBR) y(FixedString<2>, BITS) y(FixedString<10>, HEXES) y(FixedString<10>, BYTES) y(uint32_t, MSC_LOCATION) y(uint32_t, VLR_LOCATION)

#define SUBSCRIBER_GET_PLAIN_KEY_FUNC           \
        uint64_t get_plain_key() const          \
        {                                       \
                return S_ID;                    \
        }

DO_STRUCT(subscriber, SUBSCRIBER_KEY_FIELDS, SUBSCRIBER_VALUE_FIELDS, NAMESPACE_FIELDS, SUBSCRIBER_GET_PLAIN_KEY_FUNC)

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

} // namespace star

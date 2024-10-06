//
// Created by Yi Lu on 7/15/18.
//

#pragma once

// macros for code generation

#define APPLY_X_AND_Y(x, y) x(y, y)

#define NAMESPACE_OPEN(name) \
	namespace name       \
	{

#define NAMESPACE_CLOSE(name) }

#define NAMESPACE_EXPAND(name) name::

#define STRUCT_PARAM_FIRST_X(type, name) type name

#define STRUCT_PARAM_REST_X(type, name) , type name

#define STRUCT_INITLIST_FIRST_X(type, name) name(name)

#define STRUCT_INITLIST_REST_X(type, name) , name(name)

#define STRUCT_HASH_FIRST_X(type, name) k.name

#define STRUCT_HASH_REST_X(type, name) , k.name

#define STRUCT_LAYOUT_X(type, name) type name;

#define STRUCT_EQ_X(type, name)       \
	if (this->name != other.name) \
		return false;

#define STRUCT_FIELDPOS_X(type, name) name##_field,

// the main macro
#define DO_STRUCT(name, keyfields, valuefields, namespacefields, get_plain_key_func)                       \
	namespacefields(NAMESPACE_OPEN) struct name {                                                      \
		struct key {                                                                               \
			key() = default;                                                                   \
			key(keyfields(STRUCT_PARAM_FIRST_X, STRUCT_PARAM_REST_X))                          \
				: keyfields(STRUCT_INITLIST_FIRST_X, STRUCT_INITLIST_REST_X)               \
			{                                                                                  \
			}                                                                                  \
			APPLY_X_AND_Y(keyfields, STRUCT_LAYOUT_X)                                          \
			bool operator==(const struct key &other) const                                     \
			{                                                                                  \
				APPLY_X_AND_Y(keyfields, STRUCT_EQ_X)                                      \
				return true;                                                               \
			}                                                                                  \
			bool operator!=(const struct key &other) const                                     \
			{                                                                                  \
				return !operator==(other);                                                 \
			}                                                                                  \
                        get_plain_key_func                                                                 \
			enum { APPLY_X_AND_Y(keyfields, STRUCT_FIELDPOS_X) NFIELDS };                      \
		};                                                                                         \
		struct value {                                                                             \
			value() = default;                                                                 \
			value(valuefields(STRUCT_PARAM_FIRST_X, STRUCT_PARAM_REST_X))                      \
				: valuefields(STRUCT_INITLIST_FIRST_X, STRUCT_INITLIST_REST_X)             \
			{                                                                                  \
			}                                                                                  \
			APPLY_X_AND_Y(valuefields, STRUCT_LAYOUT_X)                                        \
			bool operator==(const struct value &other) const                                   \
			{                                                                                  \
				APPLY_X_AND_Y(valuefields, STRUCT_EQ_X)                                    \
				return true;                                                               \
			}                                                                                  \
			bool operator!=(const struct value &other) const                                   \
			{                                                                                  \
				return !operator==(other);                                                 \
			}                                                                                  \
			enum { APPLY_X_AND_Y(valuefields, STRUCT_FIELDPOS_X) NFIELDS };                    \
		};                                                                                         \
                struct KeyComparator {                                                                     \
                        int operator()(const key &a, const key &b) const                                   \
                        {                                                                                  \
                                if (a.get_plain_key() > b.get_plain_key())                                 \
                                        return 1;                                                          \
                                else if (a.get_plain_key() == b.get_plain_key())                           \
                                        return 0;                                                          \
                                else                                                                       \
                                        return -1;                                                         \
                        }                                                                                  \
                };                                                                                         \
                struct ValueComparator {                                                                   \
                        int operator()(const value &a, const value &b) const                               \
                        {                                                                                  \
                                if (a != b)                                                                \
                                        return 1;                                                          \
                                else                                                                       \
                                        return 0;                                                          \
                        }                                                                                  \
                };                                                                                         \
		static constexpr std::size_t tableID = __COUNTER__ - __BASE_COUNTER__;                     \
	};                                                                                                 \
	namespacefields(NAMESPACE_CLOSE) namespace std                                                     \
	{                                                                                                  \
		template <> struct hash<namespacefields(NAMESPACE_EXPAND) name::key> {                     \
			std::size_t operator()(const namespacefields(NAMESPACE_EXPAND) name::key &k) const \
			{                                                                                  \
				return star::hash(keyfields(STRUCT_HASH_FIRST_X, STRUCT_HASH_REST_X));     \
			}                                                                                  \
		};                                                                                         \
	}

// the main macro
#define DO_STRUCT_WITH_CUSTOME_KEY_COMPARATOR(name, keyfields, valuefields, namespacefields, get_plain_key_func, custom_key_comparator) \
	namespacefields(NAMESPACE_OPEN) struct name {                                                                                   \
		struct key {                                                                                                            \
			key() = default;                                                                                                \
			key(keyfields(STRUCT_PARAM_FIRST_X, STRUCT_PARAM_REST_X))                                                       \
				: keyfields(STRUCT_INITLIST_FIRST_X, STRUCT_INITLIST_REST_X)                                            \
			{                                                                                                               \
			}                                                                                                               \
			APPLY_X_AND_Y(keyfields, STRUCT_LAYOUT_X)                                                                       \
			bool operator==(const struct key &other) const                                                                  \
			{                                                                                                               \
				APPLY_X_AND_Y(keyfields, STRUCT_EQ_X)                                                                   \
				return true;                                                                                            \
			}                                                                                                               \
			bool operator!=(const struct key &other) const                                                                  \
			{                                                                                                               \
				return !operator==(other);                                                                              \
			}                                                                                                               \
                        get_plain_key_func                                                                                              \
			enum { APPLY_X_AND_Y(keyfields, STRUCT_FIELDPOS_X) NFIELDS };                                                   \
		};                                                                                                                      \
		struct value {                                                                                                          \
			value() = default;                                                                                              \
			value(valuefields(STRUCT_PARAM_FIRST_X, STRUCT_PARAM_REST_X))                                                   \
				: valuefields(STRUCT_INITLIST_FIRST_X, STRUCT_INITLIST_REST_X)                                          \
			{                                                                                                               \
			}                                                                                                               \
			APPLY_X_AND_Y(valuefields, STRUCT_LAYOUT_X)                                                                     \
			bool operator==(const struct value &other) const                                                                \
			{                                                                                                               \
				APPLY_X_AND_Y(valuefields, STRUCT_EQ_X)                                                                 \
				return true;                                                                                            \
			}                                                                                                               \
			bool operator!=(const struct value &other) const                                                                \
			{                                                                                                               \
				return !operator==(other);                                                                              \
			}                                                                                                               \
			enum { APPLY_X_AND_Y(valuefields, STRUCT_FIELDPOS_X) NFIELDS };                                                 \
		};                                                                                                                      \
                custom_key_comparator                                                                                                   \
                struct ValueComparator {                                                                                                \
                        int operator()(const value &a, const value &b) const                                                            \
                        {                                                                                                               \
                                if (a != b)                                                                                             \
                                        return 1;                                                                                       \
                                else                                                                                                    \
                                        return 0;                                                                                       \
                        }                                                                                                               \
                };                                                                                                                      \
		static constexpr std::size_t tableID = __COUNTER__ - __BASE_COUNTER__;                                                  \
	};                                                                                                                              \
	namespacefields(NAMESPACE_CLOSE) namespace std                                                                                  \
	{                                                                                                                               \
		template <> struct hash<namespacefields(NAMESPACE_EXPAND) name::key> {                                                  \
			std::size_t operator()(const namespacefields(NAMESPACE_EXPAND) name::key &k) const                              \
			{                                                                                                               \
				return star::hash(keyfields(STRUCT_HASH_FIRST_X, STRUCT_HASH_REST_X));                                  \
			}                                                                                                               \
		};                                                                                                                      \
	}

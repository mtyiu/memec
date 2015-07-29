#ifndef __COMMON_MAP_MAP_HH__
#define __COMMON_MAP_MAP_HH__

#include <map>
#include "../ds/key.hh"
#include "../ds/key_value.hh"

typedef struct {
	std::map<Key, KeyValue> keyValue;
} Map;

#endif

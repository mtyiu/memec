#ifndef __SLAVE_MAP_MAP_HH__
#define __SLAVE_MAP_MAP_HH__

#include <map>
#include <vector>
#include "../../common/ds/key.hh"
#include "../../common/ds/key_value.hh"
#include "../../common/ds/metadata.hh"
#include "../../common/protocol/protocol.hh"

typedef struct {
	std::map<Key, Metadata> metadata;
	std::map<Key, KeyValue> cache;

	// Store the keys to be synchronized with coordinator
	std::map<Key, Metadata> ops;
} Map;

#endif

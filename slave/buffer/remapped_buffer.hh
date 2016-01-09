#ifndef __SLAVE_BUFFER_REMAPPED_BUFFER_HH__
#define __SLAVE_BUFFER_REMAPPED_BUFFER_HH__

#include "../../common/ds/key_value.hh"
#include "../../common/lock/lock.hh"

struct RemappedKeyValue {
	uint32_t listId, chunkId;
	uint32_t *original, *remapped, remappedCount;
	uint8_t keySize;
	uint32_t valueSize;
	char *key, *value;
};

class RemappedBuffer {
public:
	std::unordered_map<Key, RemappedKeyValue> keys;
	LOCK_T keysLock;

	RemappedBuffer();
	bool insert(
		uint32_t listId, uint32_t chunkId,
		uint32_t *original, uint32_t *remapped, uint32_t remappedCount,
		char *key, uint8_t keySize,
		char *value, uint32_t valueSize
	);
	bool find( uint8_t keySize, char *keyStr, RemappedKeyValue *remappedKeyValue = 0 );
};

#endif

#ifndef __SERVER_BUFFER_REMAPPED_BUFFER_HH__
#define __SERVER_BUFFER_REMAPPED_BUFFER_HH__

#include "../../common/ds/key_value.hh"
#include "../../common/lock/lock.hh"

struct RemappedKeyValue {
	uint32_t listId, chunkId;
	uint32_t *original, *remapped, remappedCount;
	KeyValue keyValue;
};

struct RemappingRecord {
	uint32_t listId, chunkId;
	uint32_t *original, *remapped, remappedCount;
	Key key;
};

class RemappedBuffer {
public:
	std::unordered_map<Key, RemappedKeyValue> keys;
	std::unordered_map<Key, RemappingRecord> records;
	LOCK_T keysLock;
	LOCK_T recordsLock;

	RemappedBuffer();
	bool insert(
		uint32_t listId, uint32_t chunkId,
		uint32_t *original, uint32_t *remapped, uint32_t remappedCount,
		char *key, uint8_t keySize,
		char *value, uint32_t valueSize
	);
	bool insert(
		uint32_t listId, uint32_t chunkId,
		uint32_t *original, uint32_t *remapped, uint32_t remappedCount,
		char *key, uint8_t keySize
	);
	bool find( uint8_t keySize, char *keyStr, RemappedKeyValue *remappedKeyValue = 0 );
	bool find( uint8_t keySize, char *keyStr, RemappingRecord *remappingRecord = 0 );
	bool update(
		uint8_t keySize, char *keyStr,
		uint32_t valueUpdateSize, uint32_t valueUpdateOffset, char *valueUpdate,
		RemappedKeyValue *remappedKeyValue = 0
	);

	std::unordered_map<Key, RemappedKeyValue>::iterator erase( std::unordered_map<Key, RemappedKeyValue>::iterator it, bool needsLock = false, bool needsUnlock = false );
};

#endif

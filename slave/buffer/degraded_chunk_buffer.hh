#ifndef __SLAVE_BUFFER_DEGRADED_CHUNK_BUFFER_HH__
#define __SLAVE_BUFFER_DEGRADED_CHUNK_BUFFER_HH__

#include "../ds/map.hh"
#include "chunk_buffer.hh"

class DegradedMap {
private:
	/**
	 * Store the mapping between keys and chunks
	 * Key |-> (list ID, stripe ID, chunk ID, offset, length)
	 */
	std::unordered_map<Key, KeyMetadata> keys;
	LOCK_T keysLock;
	/**
	 * Store the key-value pairs from unsealed chunks
	 * Key |-> KeyValue
	 */
	std::unordered_map<Key, KeyValue> values;
	std::unordered_map<Key, Metadata> valueMeta;
	LOCK_T valuesLock;
	/**
	 * Store the cached chunks
	 * (list ID, stripe ID, chunk ID) |-> Chunk *
	 */
	std::unordered_map<Metadata, Chunk *> cache;
	LOCK_T cacheLock;
	struct {
		/**
		 * Store the set of reconstructed chunks and the list of IDs of pending requests
		 * (list ID, stripe ID, chunk ID) |-> pid.id
		 */
		std::unordered_map<Metadata, std::vector<uint32_t>> chunks;
		LOCK_T chunksLock;
		/**
		 * Store the set of keys in unsealed chunks and the list of IDs of pending requests
		 * Key |-> pid.id
		 */
		std::unordered_map<Key, std::vector<uint32_t>> keys;
		LOCK_T keysLock;
	} degraded;

	Map *slaveMap;

public:
	DegradedMap();

	void init( Map *map );

	bool findValueByKey(
		char *data, uint8_t size,
		bool &isSealed,
		KeyValue *keyValue,
		Key *keyPtr = 0,
		KeyMetadata *keyMetadataPtr = 0,
		Metadata *metadataPtr = 0,
		Chunk **chunkPtr = 0
	);

	Chunk *findChunkById(
		uint32_t listId, uint32_t stripeId, uint32_t chunkId,
		Metadata *metadataPtr = 0
	);

	bool insertKey( Key key, uint8_t opcode, KeyMetadata &keyMetadata );
	bool deleteKey(
		Key key, uint8_t opcode, KeyMetadata &keyMetadata,
		bool needsLock, bool needsUnlock
	);

	bool insertValue( KeyValue keyValue, Metadata metadata );
	bool deleteValue( Key key, uint8_t opcode );

	bool insertDegradedChunk(
		uint32_t listId, uint32_t stripeId, uint32_t chunkId,
		uint32_t pid
	);
	bool deleteDegradedChunk(
		uint32_t listId, uint32_t stripeId, uint32_t chunkId,
		std::vector<uint32_t> &pids
	);

	bool insertDegradedKey( Key key, uint32_t pid );
	bool deleteDegradedKey( Key key, std::vector<uint32_t> &pids );

	bool insertChunk(
		uint32_t listId, uint32_t stripeId, uint32_t chunkId,
		Chunk *chunk, bool isParity = false
	);
	Chunk *deleteChunk( uint32_t listId, uint32_t stripeId, uint32_t chunkId, Metadata *metadataPtr = 0 );

	void getKeysMap( std::unordered_map<Key, KeyMetadata> *&keys, LOCK_T *&lock );
	void getCacheMap( std::unordered_map<Metadata, Chunk *> *&cache, LOCK_T *&lock );

	void dump( FILE *f = stdout );
};

class DegradedChunkBuffer : public ChunkBuffer {
public:
	DegradedMap map;

	DegradedChunkBuffer();

	void print( FILE *f = stdout );
	void stop();

	bool updateKeyValue( uint8_t keySize, char *keyStr, uint32_t valueUpdateSize, uint32_t valueUpdateOffset, uint32_t chunkUpdateOffset, char *valueUpdate, Chunk *chunk, bool isSealed );
	bool deleteKey( uint8_t opcode, uint8_t keySize, char *keyStr, bool isSealed, uint32_t &deltaSize, char *delta, Chunk *chunk );

	~DegradedChunkBuffer();
};

#endif

#ifndef __SLAVE_BUFFER_DEGRADED_CHUNK_BUFFER_HH__
#define __SLAVE_BUFFER_DEGRADED_CHUNK_BUFFER_HH__

#include "../ds/map.hh"
#include "chunk_buffer.hh"

struct pid_s {
	uint16_t instanceId;
	uint32_t requestId;
};

class DegradedMap {
private:
	/**
	 * Store the mapping between keys and chunks
	 * Key |-> (list ID, stripe ID, chunk ID, offset, length)
	 */
	std::unordered_map<Key, KeyMetadata> keys;
	LOCK_T keysLock;
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
		std::unordered_map<Metadata, std::vector<struct pid_s>> chunks;
		std::unordered_set<Metadata> reconstructedChunks;
		LOCK_T chunksLock;
		/**
		 * Store the set of keys in unsealed chunks and the list of IDs of pending requests
		 * Key |-> pid.id
		 */
		std::unordered_map<Key, std::vector<struct pid_s>> keys;
		std::unordered_set<Key> reconstructedKeys;
		LOCK_T keysLock;
	} degraded;

	/**
	 * Store the parity chunks reconstructed in the data server (either redirected or not)
	 * (list ID, stripe ID, data chunk ID) |-> vector of parity chunk IDs
	 */
	std::unordered_map<Metadata, std::unordered_set<uint32_t>> parity;
	LOCK_T parityLock;

	Map *slaveMap;

public:
	/**
	 * Store the key-value pairs from unsealed chunks
	 * Key |-> KeyValue
	 */
	struct {
		std::unordered_map<Key, KeyValue> values;
		std::unordered_multimap<Metadata, Key> metadataRev;
		std::unordered_set<Key> deleted;
		LOCK_T lock;
	} unsealed;

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
		Metadata *metadataPtr = 0,
		bool needsLock = true, bool needsUnlock = true
	);

	bool insertKey( Key key, uint8_t opcode, KeyMetadata &keyMetadata );
	bool deleteKey(
		Key key, uint8_t opcode, uint32_t &timestamp, KeyMetadata &keyMetadata,
		bool needsLock, bool needsUnlock
	);

	bool insertValue( KeyValue keyValue, Metadata metadata );
	bool deleteValue( Key key, Metadata metadata, uint8_t opcode, uint32_t &timestamp );

	bool insertDegradedChunk(
		uint32_t listId, uint32_t stripeId, uint32_t chunkId,
		uint16_t instanceId, uint32_t requestId, bool &isReconstructed
	);
	bool deleteDegradedChunk(
		uint32_t listId, uint32_t stripeId, uint32_t chunkId,
		std::vector<struct pid_s> &pids
	);

	bool insertDegradedKey( Key key, uint16_t instanceId, uint32_t requestId, bool &isReconstructed );
	bool deleteDegradedKey( Key key, std::vector<struct pid_s> &pids );

	bool insertChunk(
		uint32_t listId, uint32_t stripeId, uint32_t chunkId,
		Chunk *chunk, bool isParity = false,
		bool needsLock = true, bool needsUnlock = true
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

	// For reconstructed data chunks
	bool updateKeyValue(
		uint8_t keySize, char *keyStr,
		uint32_t valueUpdateSize, uint32_t valueUpdateOffset, uint32_t chunkUpdateOffset,
		char *valueUpdate, Chunk *chunk, bool isSealed
	);
	bool deleteKey(
		uint8_t opcode, uint32_t &timestamp,
		uint8_t keySize, char *keyStr, Metadata metadata, bool isSealed,
		uint32_t &deltaSize, char *delta, Chunk *chunk
	);

	// For reconstructed parity chunks
	bool update(
		uint32_t listId, uint32_t stripeId, uint32_t chunkId, uint32_t updatingChunkId,
		uint32_t offset, uint32_t size, char *dataDelta,
		Chunk **dataChunks, Chunk *dataChunk, Chunk *parityChunk
	);

	~DegradedChunkBuffer();
};

#endif

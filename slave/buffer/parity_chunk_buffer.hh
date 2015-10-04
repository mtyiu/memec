#ifndef __SLAVE_BUFFER_PARITY_CHUNK_BUFFER_HH__
#define __SLAVE_BUFFER_PARITY_CHUNK_BUFFER_HH__

#include <set>
#include "chunk_buffer.hh"
#include "../../common/ds/bitmask_array.hh"

class ParityChunkWrapper {
public:
	uint32_t pending;
	pthread_mutex_t lock;
	Chunk *chunk;

	ParityChunkWrapper();
};

class KeyValueOffset {
public:
	uint32_t stripeId, offset;
};

class ParityChunkBuffer : public ChunkBuffer {
private:
	// Map stripe ID to ParityChunk objects
	std::map<uint32_t, ParityChunkWrapper> chunks;
	// Temporary map that stores the not-yet-sealed key-value pairs
	std::map<Key, KeyValue> keys;
	// Keys that should be sealed but are not yet received
	std::map<Key, KeyValueOffset> pending;

	void update( uint32_t stripeId, uint32_t chunkId, uint32_t offset, uint32_t size, Chunk **dataChunks, Chunk *dataChunk, Chunk *parityChunk, bool needsLock = true, bool needsUnlock = true );

public:
	ParityChunkBuffer( uint32_t count, uint32_t listId, uint32_t stripeId, uint32_t chunkId );
	ParityChunkWrapper &getWrapper( uint32_t stripeId, bool needsLock = true, bool needsUnlock = true );
	bool set( char *key, uint8_t keySize, char *value, uint32_t valueSize, uint32_t chunkId, Chunk **dataChunks, Chunk *dataChunk, Chunk *parityChunk );
	bool seal( uint32_t stripeId, uint32_t chunkId, uint32_t count, char *sealData, size_t sealDataSize, Chunk **dataChunks, Chunk *dataChunk, Chunk *parityChunk );
	void update( uint32_t stripeId, uint32_t chunkId, uint32_t offset, uint32_t size, char *dataDelta, Chunk **dataChunks, Chunk *dataChunk, Chunk *parityChunk );
	void flush( uint32_t stripeId, Chunk *chunk );
	void print( FILE *f = stdout );
	void stop();
};

#endif

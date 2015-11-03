#ifndef __SLAVE_BUFFER_PARITY_CHUNK_BUFFER_HH__
#define __SLAVE_BUFFER_PARITY_CHUNK_BUFFER_HH__

#include <unordered_map>
#include "chunk_buffer.hh"
#include "../../common/ds/bitmask_array.hh"

class ParityChunkWrapper {
public:
	bool *pending;
	LOCK_T lock;
	Chunk *chunk;

	ParityChunkWrapper();
	uint32_t countPending();
	void free();
};

enum PendingRequestType {
	PRT_SEAL,
	PRT_UPDATE,
	PRT_DELETE
};

class PendingRequest {
public:
	PendingRequestType type;
	union {
		struct {
			uint32_t stripeId, offset;
		} seal;
		struct {
			uint32_t offset, length;
			char *buf;
		} update;
	} req;

	void seal( uint32_t stripeId, uint32_t offset ) {
		this->type = PRT_SEAL;
		this->req.seal.stripeId = stripeId;
		this->req.seal.offset = offset;
	}

	void update( uint32_t offset, uint32_t length, char *update ) {
		this->type = PRT_UPDATE;
		this->req.update.offset = offset;
		this->req.update.length = length;
		this->req.update.buf = new char[ length ];
		memcpy( this->req.update.buf, update, length );
	}

	void del() {
		this->type = PRT_DELETE;
	}
};

class KeyValueOffset {
public:
	uint32_t stripeId, offset;
};

class ParityChunkBuffer : public ChunkBuffer {
private:
	// Map stripe ID to ParityChunk objects
	std::unordered_map<uint32_t, ParityChunkWrapper> chunks;
	// Temporary map that stores the not-yet-sealed key-value pairs
	std::unordered_map<Key, KeyValue> keys;
	// Store the request that update the not-yet-received keys
	std::unordered_map<Key, PendingRequest> pending;

	void update( uint32_t stripeId, uint32_t chunkId, uint32_t offset, uint32_t size, Chunk **dataChunks, Chunk *dataChunk, Chunk *parityChunk, bool needsLock = true, bool needsUnlock = true, bool isSeal = false, bool isDelete = false );

public:
	ParityChunkBuffer( uint32_t count, uint32_t listId, uint32_t stripeId, uint32_t chunkId );
	ParityChunkWrapper &getWrapper( uint32_t stripeId, bool needsLock = true, bool needsUnlock = true );
	bool set( char *key, uint8_t keySize, char *value, uint32_t valueSize, uint32_t chunkId, Chunk **dataChunks, Chunk *dataChunk, Chunk *parityChunk );
	bool seal( uint32_t stripeId, uint32_t chunkId, uint32_t count, char *sealData, size_t sealDataSize, Chunk **dataChunks, Chunk *dataChunk, Chunk *parityChunk );
	bool deleteKey( char *keyStr, uint8_t keySize );
	bool updateKeyValue( char *keyStr, uint8_t keySize, uint32_t offset, uint32_t length, char *valueUpdate );
	void update( uint32_t stripeId, uint32_t chunkId, uint32_t offset, uint32_t size, char *dataDelta, Chunk **dataChunks, Chunk *dataChunk, Chunk *parityChunk, bool isDelete = false );
	void print( FILE *f = stdout );
	void stop();
};

#endif

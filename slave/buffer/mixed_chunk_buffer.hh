#ifndef __SLAVE_BUFFER_MIXED_CHUNK_BUFFER_HH__
#define __SLAVE_BUFFER_MIXED_CHUNK_BUFFER_HH__

#include <cstdio>
#include "data_chunk_buffer.hh"
#include "parity_chunk_buffer.hh"

enum ChunkBufferRole {
	CBR_DATA,
	CBR_PARITY
};

class MixedChunkBuffer {
public:
	ChunkBufferRole role;
	union {
		DataChunkBuffer *data;
		ParityChunkBuffer *parity;
	} buffer;

	MixedChunkBuffer( DataChunkBuffer *dataChunkBuffer );
	MixedChunkBuffer( ParityChunkBuffer *parityChunkBuffer );

	bool set( SlaveWorker *worker, char *key, uint8_t keySize, char *value, uint32_t valueSize, uint8_t opcode, uint32_t chunkId, Chunk **chunks = 0, Chunk *dataChunk = 0, Chunk *parityChunk = 0 );

	// For DataChunkBuffer only
	size_t seal( SlaveWorker *worker );
	bool reInsert( SlaveWorker *worker, Chunk *chunk, uint32_t sizeToBeFreed, bool needsLock, bool needsUnlock );
	// For ParityChunkBuffer only
	bool seal( uint32_t stripeId, uint32_t chunkId, uint32_t count, char *sealData, size_t sealDataSize, Chunk **dataChunks, Chunk *dataChunk, Chunk *parityChunk );

	inline uint32_t getChunkId() { return this->role == CBR_DATA ? this->buffer.data->getChunkId() : this->buffer.parity->getChunkId(); }

	int lockChunk( Chunk *chunk, bool keepGlobalLock = false );
	void updateAndUnlockChunk( int index );
	void unlock();
	bool deleteKey( char *keyStr, uint8_t keySize );
	bool updateKeyValue( char *keyStr, uint8_t keySize, uint32_t offset, uint32_t length, char *valueUpdate );
	void update( uint32_t stripeId, uint32_t chunkId, uint32_t offset, uint32_t size, char *dataDelta, Chunk **dataChunks, Chunk *dataChunk, Chunk *parityChunk );
	void print( FILE *f = stdout );
	void stop();
	~MixedChunkBuffer();
};

#endif

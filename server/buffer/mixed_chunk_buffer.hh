#ifndef __SERVER_BUFFER_MIXED_CHUNK_BUFFER_HH__
#define __SERVER_BUFFER_MIXED_CHUNK_BUFFER_HH__

#include <cstdio>
#include "data_chunk_buffer.hh"
#include "parity_chunk_buffer.hh"
#include "get_chunk_buffer.hh"

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

	bool set(
		ServerWorker *worker,
		char *key, uint8_t keySize,
		char *value, uint32_t valueSize,
		uint8_t opcode, uint32_t &timestamp,
		uint32_t &stripeId, uint32_t chunkId, uint32_t splitOffset,
		uint8_t *sealedCount, Metadata *sealed1, Metadata *sealed2,
		Chunk **chunks, Chunk *dataChunk, Chunk *parityChunk,
		GetChunkBuffer *getChunkBuffer
	);

	// For DataChunkBuffer only
	void init();
	size_t seal( ServerWorker *worker );
	bool reInsert( ServerWorker *worker, Chunk *chunk, uint32_t sizeToBeFreed, bool needsLock, bool needsUnlock );
	// For ParityChunkBuffer only
	bool seal( uint32_t stripeId, uint32_t chunkId, uint32_t count, char *sealData, size_t sealDataSize, Chunk **dataChunks, Chunk *dataChunk, Chunk *parityChunk );

	inline uint32_t getChunkId() { return this->role == CBR_DATA ? this->buffer.data->getChunkId() : this->buffer.parity->getChunkId(); }

	int lockChunk( Chunk *chunk, bool keepGlobalLock = false );
	void updateAndUnlockChunk( int index );
	void unlock( int index = -1 );

	bool findValueByKey( char *data, uint8_t size, bool isLarge, KeyValue *keyValuePtr, Key *keyPtr = 0, bool verbose = false );
	bool getKeyValueMap( std::unordered_map<Key, KeyValue> *&map, LOCK_T *&lock, bool verbose = false );

	bool deleteKey( char *keyStr, uint8_t keySize );

	bool updateKeyValue( char *keyStr, uint8_t keySize, bool isLarge, uint32_t offset, uint32_t length, char *valueUpdate );

	bool update( uint32_t stripeId, uint32_t chunkId, uint32_t offset, uint32_t size, char *dataDelta, Chunk **dataChunks, Chunk *dataChunk, Chunk *parityChunk, bool isDelete = false );

	bool *getSealIndicator( uint32_t stripeId, uint8_t &sealIndicatorCount, bool needsLock, bool needsUnlock, LOCK_T **lock );

	void print( FILE *f = stdout );
	void stop();

	~MixedChunkBuffer();
};

#endif

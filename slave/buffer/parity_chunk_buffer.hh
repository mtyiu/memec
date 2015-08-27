#ifndef __SLAVE_BUFFER_PARITY_CHUNK_BUFFER_HH__
#define __SLAVE_BUFFER_PARITY_CHUNK_BUFFER_HH__

#include <set>
#include "chunk_buffer.hh"
#include "dummy_data_chunk_buffer.hh"
#include "../../common/ds/bitmask_array.hh"

class ParityChunkWrapper {
public:
	uint32_t pending;
	pthread_mutex_t lock;
	Chunk *chunk;

	ParityChunkWrapper();
};

class ParityChunkBuffer : public ChunkBuffer {
private:
	// k data chunk buffer in total to emulate the status in each data slave
	DummyDataChunkBuffer **dummyDataChunkBuffer;
	// Map stripe ID to ParityChunk objects
	std::map<uint32_t, ParityChunkWrapper> chunks;

public:
	ParityChunkBuffer( uint32_t count, uint32_t listId, uint32_t stripeId, uint32_t chunkId );
	ParityChunkWrapper &getWrapper( uint32_t stripeId );
	void set( char *key, uint8_t keySize, char *value, uint32_t valueSize, uint32_t chunkId, Chunk **dataChunks, Chunk *dataChunk, Chunk *parityChunk );
	void flush( uint32_t stripeId, Chunk *chunk );
	void print( FILE *f = stdout );
	void stop();
	~ParityChunkBuffer();

	void flushData( uint32_t stripeId );
	static void dataChunkFlushHandler( uint32_t stripeId, void *argv );
};

#endif

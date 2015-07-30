#ifndef __SLAVE_BUFFER_PARITY_CHUNK_BUFFER_HH__
#define __SLAVE_BUFFER_PARITY_CHUNK_BUFFER_HH__

#include "chunk_buffer.hh"
#include "data_chunk_buffer.hh"
#include "../../common/ds/bitmask_array.hh"

class ParityChunkBuffer : public ChunkBuffer {
private:
	uint32_t dataChunkCount;  // Number of data chunks per stripe
	Chunk ***dataChunks;      // Use dataChunks[ chunkId ][ stripe index ] to retrieve a chunk in a stripe
	DataChunkBuffer **dataChunkBuffer;
	BitmaskArray *status;

public:
	ParityChunkBuffer( uint32_t capacity, uint32_t count, uint32_t dataChunkCount, uint32_t listId, uint32_t stripeId, uint32_t chunkId );
	KeyValue set( char *key, uint8_t keySize, char *value, uint32_t valueSize, uint32_t chunkId );
	uint32_t flush( bool lock = true );
	Chunk *flush( int index, bool lock = true );
	void print( FILE *f = stdout );
	void stop();
	~ParityChunkBuffer();

	void flushData( Chunk *chunk );
	Chunk *flushData( uint32_t chunkId, int index, bool lock = true );
	static void dataChunkFlushHandler( Chunk *chunk, void *argv );
};

#endif

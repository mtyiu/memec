#ifndef __SLAVE_BUFFER_DATA_CHUNK_BUFFER_HH__
#define __SLAVE_BUFFER_DATA_CHUNK_BUFFER_HH__

#include "chunk_buffer.hh"

class DataChunkBuffer : public ChunkBuffer {
private:
	uint32_t *sizes;           // Occupied space for each chunk
	void ( *flushFn )( Chunk *, void * );
	void *argv;

public:
	DataChunkBuffer( uint32_t capacity, uint32_t count, uint32_t listId, uint32_t stripeId, uint32_t chunkId, void ( *flushFn )( Chunk *, void * ) = 0, void *argv = 0 );
	KeyMetadata set( char *key, uint8_t keySize, char *value, uint32_t valueSize, uint32_t chunkId = 0 );
	uint32_t flush( bool lock = true );
	Chunk *flush( int index, bool lock = true );
	void print( FILE *f = stdout );
	void stop();
	~DataChunkBuffer();
};

#endif

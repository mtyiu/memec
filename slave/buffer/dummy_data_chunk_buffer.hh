#ifndef __DUMMY_DATA_CHUNK_BUFFER_HH__
#define __DUMMY_DATA_CHUNK_BUFFER_HH__

#include "chunk_buffer.hh"

class DummyDataChunk {
public:
	ChunkStatus status;
	uint32_t count;
	uint32_t size;
	Metadata metadata;

	DummyDataChunk();
	void alloc( uint32_t size, uint32_t &offset );
	void clear();
};

class DummyDataChunkBuffer {
private:
	uint32_t count;                        // Number of chunks
	uint32_t listId;                       // List ID of this buffer
	uint32_t stripeId;                     // Current stripe ID
	uint32_t chunkId;                      // Chunk ID of this buffer
	pthread_mutex_t lock;                  // Lock for the whole buffer
	pthread_mutex_t *locks;                // Lock for each chunk
	DummyDataChunk **chunks;               // Allocated chunk buffer
	uint32_t *sizes;                       // Occupied space for each chunk
	void ( *flushFn )( uint32_t, void * ); // Handler of the flush chunk event
	void *argv;                            // Arguments to the handler

public:
	DummyDataChunkBuffer( uint32_t count, uint32_t listId, uint32_t stripeId, uint32_t chunkId, void ( *flushFn )( uint32_t, void * ), void *argv );
	uint32_t set( uint32_t size, uint32_t &offset );
	uint32_t flush( bool lock = true );
	uint32_t flush( int index, bool lock = true );
	void print( FILE *f = stdout );
	~DummyDataChunkBuffer();
};

#endif

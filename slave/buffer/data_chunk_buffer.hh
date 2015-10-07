#ifndef __SLAVE_BUFFER_DATA_CHUNK_BUFFER_HH__
#define __SLAVE_BUFFER_DATA_CHUNK_BUFFER_HH__

#include "chunk_buffer.hh"

class SlaveWorker;

class DataChunkBuffer : public ChunkBuffer {
private:
	uint32_t count;                        // Number of chunks
	pthread_mutex_t *locks;                // Lock for each chunk
	Chunk **chunks;                        // Allocated chunk buffer
	uint32_t *sizes;                       // Occupied space for each chunk

public:
	DataChunkBuffer( uint32_t count, uint32_t listId, uint32_t stripeId, uint32_t chunkId );

	KeyMetadata set( SlaveWorker *worker, char *key, uint8_t keySize, char *value, uint32_t valueSize, uint8_t opcode );

	size_t seal( SlaveWorker *worker );

	int lockChunk( Chunk *chunk );
	void updateAndUnlockChunk( int index );
	uint32_t flush( SlaveWorker *worker, bool lock = true, bool lockAtIndex = false );
	Chunk *flushAt( SlaveWorker *worker, int index, bool lock = true );
	void print( FILE *f = stdout );
	void stop();
	~DataChunkBuffer();
};

#endif

#ifndef __SLAVE_BUFFER_DATA_CHUNK_BUFFER_HH__
#define __SLAVE_BUFFER_DATA_CHUNK_BUFFER_HH__

#define REINSERTED_CHUNKS_IS_SET

#ifdef REINSERTED_CHUNKS_IS_SET
#include <set>
#endif

#include "chunk_buffer.hh"

class SlaveWorker;

class DataChunkBuffer : public ChunkBuffer {
private:
	uint32_t count;                        // Number of chunks
	pthread_mutex_t *locks;                // Lock for each chunk
	Chunk **chunks;                        // Allocated chunk buffer
#ifdef REINSERTED_CHUNKS_IS_SET
	std::set<Chunk *> reInsertedChunks;    // Chunks that have free space after deletion
#else
	Chunk **reInsertedChunks;              // Chunks that have free space after deletion
#endif
	uint32_t *sizes;                       // Occupied space for each chunk
	uint32_t reInsertedChunkMaxSpace;      // Maximum space avaiable in the re-inserted chunks

public:
	DataChunkBuffer( uint32_t count, uint32_t listId, uint32_t stripeId, uint32_t chunkId );

	KeyMetadata set( SlaveWorker *worker, char *key, uint8_t keySize, char *value, uint32_t valueSize, uint8_t opcode );

	size_t seal( SlaveWorker *worker );

	// Re-insert into the buffer after a DELETE operation
	bool reInsert( SlaveWorker *worker, Chunk *chunk, uint32_t sizeToBeFreed, bool needsLock, bool needsUnlock );

	int lockChunk( Chunk *chunk, bool keepGlobalLock );
	void updateAndUnlockChunk( int index );
	void unlock();
	uint32_t flush( SlaveWorker *worker, bool lock = true, bool lockAtIndex = false );
	Chunk *flushAt( SlaveWorker *worker, int index, bool lock = true );
	void print( FILE *f = stdout );
	void stop();
	~DataChunkBuffer();
};

#endif

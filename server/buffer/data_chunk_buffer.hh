#ifndef __SLAVE_BUFFER_DATA_CHUNK_BUFFER_HH__
#define __SLAVE_BUFFER_DATA_CHUNK_BUFFER_HH__

#define REINSERTED_CHUNKS_IS_SET

#ifdef REINSERTED_CHUNKS_IS_SET
#include <unordered_set>
#endif

#include "chunk_buffer.hh"

class SlaveWorker;

class DataChunkBuffer : public ChunkBuffer {
private:
	uint32_t listId;                       // List ID of this buffer
	uint32_t stripeId;                     // Current stripe ID
	uint32_t chunkId;                      // Chunk ID of this buffer
	uint32_t count;                        // Number of chunks
	LOCK_T *locks;                         // Lock for each chunk
	Chunk **chunks;                        // Allocated chunk buffer
#ifdef REINSERTED_CHUNKS_IS_SET
	std::unordered_set<Chunk *> reInsertedChunks;    // Chunks that have free space after deletion
#else
	Chunk **reInsertedChunks;              // Chunks that have free space after deletion
#endif
	uint32_t *sizes;                       // Occupied space for each chunk
	uint32_t reInsertedChunkMaxSpace;      // Maximum space avaiable in the re-inserted chunks

public:
	DataChunkBuffer( uint32_t count, uint32_t listId, uint32_t stripeId, uint32_t chunkId, bool isReady );
	void init();

	inline uint32_t getChunkId() { return this->chunkId; }

	KeyMetadata set( SlaveWorker *worker, char *key, uint8_t keySize, char *value, uint32_t valueSize, uint8_t opcode, uint32_t &timestamp, uint32_t &stripeId, bool *isSealed = 0, Metadata *sealed = 0 );

	size_t seal( SlaveWorker *worker );

	// Re-insert into the buffer after a DELETE operation
	bool reInsert( SlaveWorker *worker, Chunk *chunk, uint32_t sizeToBeFreed, bool needsLock, bool needsUnlock );

	int lockChunk( Chunk *chunk, bool keepGlobalLock );
	void updateAndUnlockChunk( int index );
	void unlock( int index = -1 );

	uint32_t flush( SlaveWorker *worker, bool lock = true, bool lockAtIndex = false, Metadata *sealed = 0 );
	Chunk *flushAt( SlaveWorker *worker, int index, bool lock = true, Metadata *sealed = 0 );

	void print( FILE *f = stdout );
	void stop();

	~DataChunkBuffer();
};

#endif

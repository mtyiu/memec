#ifndef __SERVER_BUFFER_DATA_CHUNK_BUFFER_HH__
#define __SERVER_BUFFER_DATA_CHUNK_BUFFER_HH__

#define REINSERTED_CHUNKS_IS_SET

#ifdef REINSERTED_CHUNKS_IS_SET
#include <unordered_set>
#endif

#include "chunk_buffer.hh"

class ServerWorker;

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

	KeyMetadata set(
		ServerWorker *worker,
		char *key, uint8_t keySize,
		char *value, uint32_t valueSize,
		uint8_t opcode, uint32_t &timestamp,
		uint32_t &stripeId,  uint32_t splitOffset,
		uint8_t *sealedCount = 0, Metadata *sealed1 = 0, Metadata *sealed2 = 0
	);

	size_t seal( ServerWorker *worker );

	// Re-insert into the buffer after a DELETE operation
	bool reInsert( ServerWorker *worker, Chunk *chunk, uint32_t sizeToBeFreed, bool needsLock, bool needsUnlock );

	int lockChunk( Chunk *chunk, bool keepGlobalLock );
	void updateAndUnlockChunk( int index );
	void unlock( int index = -1 );

	uint32_t flush( ServerWorker *worker, bool lock = true, bool lockAtIndex = false, Metadata *sealed = 0 );
	Chunk *flushAt( ServerWorker *worker, int index, bool lock = true, Metadata *sealed = 0 );

	void print( FILE *f = stdout );
	void stop();

	~DataChunkBuffer();
};

#endif

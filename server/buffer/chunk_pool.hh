#ifndef __SERVER_BUFFRE_CHUNK_POOL_HH__
#define __SERVER_BUFFRE_CHUNK_POOL_HH__

#include <atomic>
#include <cstdio>
#include <cstdlib>

struct ChunkMetadata {
	uint32_t listId;
	uint32_t stripeId;
	// uint32_t chunkId;
	uint32_t size;
} __attribute__((__packed__));

#define CHUNK_METADATA_SIZE sizeof( struct ChunkMetadata )

class ChunkPool {
private:
	uint32_t chunkSize;              // Chunk size
	uint32_t total;                  // Number of chunks allocated
	std::atomic<unsigned int> count; // Current index
	char *startAddress;

public:
	ChunkPool();
	~ChunkPool();
	void init( uint32_t chunkSize, uint64_t capacity );
	char *alloc( uint32_t listId, uint32_t stripeId, uint32_t chunkId );
	void print( FILE *f = stdout );
};

#endif

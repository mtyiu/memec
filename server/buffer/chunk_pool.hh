#ifndef __SERVER_BUFFRE_CHUNK_POOL_HH__
#define __SERVER_BUFFRE_CHUNK_POOL_HH__

#include <atomic>
#include <cstdio>
#include <cstdlib>
#include "../../common/ds/chunk.hh"

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

	Chunk *alloc();
	Chunk *alloc( uint32_t listId, uint32_t stripeId, uint32_t chunkId );

	void setChunk( Chunk *chunk, uint32_t listId, uint32_t stripeId, uint32_t chunkId, uint32_t size = 0 );
	Chunk *getChunk( char *ptr, uint32_t *listIdPtr = 0, uint32_t *stripeIdPtr = 0, uint32_t *sizePtr = 0, uint32_t *offsetPtr = 0 ); // Translate object pointer to chunk pointer

	void print( FILE *f = stdout );
};

#endif

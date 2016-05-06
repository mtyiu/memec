#ifndef __COMMON_DS_CHUNK_POOL_HH__
#define __COMMON_DS_CHUNK_POOL_HH__

#include <atomic>
#include <cstdio>
#include <cstdlib>
#include <cassert>
#include "../../common/ds/chunk.hh"
#include "../../common/hash/hash_func.hh"
#include "../../common/lock/lock.hh"

class ChunkPool {
private:
	uint32_t total;                  // Number of chunks allocated
	std::atomic<unsigned int> count; // Current index
	char *startAddress;

public:
	ChunkPool();
	~ChunkPool();

	void init( uint32_t chunkSize, uint64_t capacity );

	Chunk *alloc( uint32_t listId = 0, uint32_t stripeId = 0, uint32_t chunkId = 0 );

	// Translate object pointer to chunk pointer
	Chunk *getChunk( char *ptr, uint32_t &offset );

	// Check whether the chunk is allocated by this chunk pool
	bool isInChunkPool( Chunk *chunk );

	void print( FILE *f = stdout );
};

class TempChunkPool {
public:
	Chunk *alloc( uint32_t listId = 0, uint32_t stripeId = 0, uint32_t chunkId = 0 ) {
		Chunk *chunk = ( Chunk * ) malloc( CHUNK_IDENTIFIER_SIZE + ChunkUtil::chunkSize );

		if ( chunk ) {
			ChunkUtil::clear( chunk );
			ChunkUtil::set( chunk, listId, stripeId, chunkId );
		}
		return chunk;
	}

	void free( Chunk *chunk ) {
		::free( ( char * ) chunk );
	}
};

#endif

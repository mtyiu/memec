#ifndef __COMMON_DS_STRIPE_HH__
#define __COMMON_DS_STRIPE_HH__

#include "chunk.hh"

class Stripe {
public:
	Chunk **chunks;
	static uint32_t dataChunkCount;
	static uint32_t parityChunkCount;

	Stripe();
	~Stripe();
	void set( Chunk **dataChunks, Chunk *parityChunk, uint32_t parityChunkId );
	void set( Chunk **dataChunks, Chunk **parityChunks );
	void set( Chunk *dataChunk, uint32_t dataChunkId, Chunk *parityChunk, uint32_t parityChunkId );
	void get( Chunk **&dataChunks, Chunk **&parityChunks );
	void get( Chunk **&dataChunks, Chunk *&parityChunk, uint32_t parityChunkId );
	uint32_t get( Chunk **&dataChunks, Chunk *&parityChunk );
	uint32_t getMaxDataChunkSize();

	static void init( uint32_t dataChunkCount, uint32_t parityChunkCount );
};

#endif

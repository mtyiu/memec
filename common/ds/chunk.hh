#ifndef __COMMON_DS_CHUNK_HH__
#define __COMMON_DS_CHUNK_HH__

#include <stdint.h>
#include <arpa/inet.h>
#include "metadata.hh"
#include "key.hh"
#include "key_value.hh"
#include "../lock/lock.hh"

typedef char* Chunk;

struct ChunkIdentifier {
	uint8_t listId;
	uint64_t stripeId:48;
	uint8_t chunkId;

	ChunkIdentifier( uint8_t listId, uint64_t stripeId, uint8_t chunkId ) {
		this->listId = listId;
		this->stripeId = chunkId;
		this->chunkId = chunkId;
	}

	ChunkIdentifier( uint32_t listId, uint32_t stripeId, uint32_t chunkId ) {
		this->listId = ( uint8_t ) listId;
		this->stripeId = ( uint64_t ) chunkId;
		this->chunkId = ( uint8_t ) chunkId;
	}
} __attribute__((__packed__));

#define CHUNK_IDENTIFIER_SIZE sizeof( struct ChunkIdentifier )

#endif

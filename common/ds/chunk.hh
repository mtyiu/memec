#ifndef __COMMON_DS_CHUNK_HH__
#define __COMMON_DS_CHUNK_HH__

#include <stdint.h>
#include <arpa/inet.h>
#include "metadata.hh"

enum ChunkStatus {
	CHUNK_STATUS_EMPTY, // Clean chunk (used in chunk buffer)
	CHUNK_STATUS_DIRTY, // Some data is written and the chunk should be flushed later
	CHUNK_STATUS_CACHED // The chunk contains data that is already flushed to disk
};

class Chunk {
public:
	static uint32_t capacity;   // Chunk size
	ChunkStatus status;			// Current status of the chunk
	uint32_t count;             // Number of key-value pair
	uint32_t size;              // Occupied data
	Metadata metadata;          // Metadata (list ID, stripe ID, chunk ID)
	bool isParity;              // Indicate whether the chunk is a parity chunk
	char *data;                 // Buffer

	Chunk();
	static void init( uint32_t capacity );
	void init();
	char *alloc( uint32_t size, uint32_t &offset );
	void updateData();
	void updateParity( uint32_t offset = 0, uint32_t length = 0 );
	void clear();
	void free();

	static bool initFn( Chunk *chunk, void *argv );
};

#endif

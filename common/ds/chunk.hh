#ifndef __COMMON_DS_CHUNK_HH__
#define __COMMON_DS_CHUNK_HH__

#include <stdint.h>
#include <arpa/inet.h>

class Chunk {
public:
	static uint32_t capacity;   // Chunk size
	uint32_t count;             // Number of key-value pair
	uint32_t size;              // Occupied data
	uint32_t listId;            // Current list ID
	uint32_t stripeId;          // Current stripe ID
	uint32_t chunkId;           // Current chunk ID
	bool isParity;              // Indiate whether this chunk is a parity chunk
	char *data;                 // Buffer

	Chunk();
	static void init( uint32_t capacity );
	void init();
	char *alloc( uint32_t size );
	void updateData();
	void updateParity( uint32_t offset = 0, uint32_t length = 0 );
	void clear();
	void free();

	static bool initFn( Chunk *chunk, void *argv );
};

#endif

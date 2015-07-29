#ifndef __COMMON_DS_CHUNK_HH__
#define __COMMON_DS_CHUNK_HH__

#include <stdint.h>
#include <arpa/inet.h>

class Chunk {
public:
	uint32_t count;             // Number of key-value pair
	uint32_t size;              // Occupied data
	char *data;

	Chunk();
	void init( uint32_t capacity );
	char *alloc( uint32_t size );
	void free();
	char *serialize();
	char *deserialize();

	static bool initFn( Chunk *chunk, void *argv );
};

#endif

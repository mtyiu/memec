#ifndef __SLAVE_BUFFER_CHUNK_BUFFER_HH__
#define __SLAVE_BUFFER_CHUNK_BUFFER_HH__

#include <pthread.h>
#include "../../common/ds/chunk.hh"
#include "../../common/ds/key_value.hh"

class ChunkBuffer {
protected:
	uint32_t capacity;        // Chunk size
	uint32_t count;           // Number of chunks
	Chunk *chunks;            // Allocated chunk buffer
	pthread_mutex_t *locks;   // Lock for each chunk

public:
	bool isParity;            // Indicate whether the buffer is for data chunks or parity chunks

	ChunkBuffer( uint32_t capacity, uint32_t count );
	virtual size_t flush( bool lock = true ) = 0;
	virtual void stop() = 0;
	virtual ~ChunkBuffer();
};

#endif

#ifndef __SLAVE_BUFFER_CHUNK_BUFFER_HH__
#define __SLAVE_BUFFER_CHUNK_BUFFER_HH__

#include <pthread.h>
#include "../../common/ds/chunk.hh"

class ChunkBuffer {
private:
	uint32_t capacity;
	uint32_t count;
	uint32_t *sizes;
	Chunk *chunks;
	pthread_mutex_t *locks;

public:
	ChunkBuffer( uint32_t capacity, uint32_t chunksPerList );
	~ChunkBuffer();
	void stop();
};

#endif

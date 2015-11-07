#ifndef __SLAVE_BUFFER_CHUNK_BUFFER_HH__
#define __SLAVE_BUFFER_CHUNK_BUFFER_HH__

#include <cstdio>
#include <pthread.h>
#include "../ds/map.hh"
#include "../event/event_queue.hh"
#include "../../common/coding/coding.hh"
#include "../../common/ds/chunk.hh"
#include "../../common/ds/memory_pool.hh"
#include "../../common/lock/lock.hh"

#define CHUNK_BUFFER_FLUSH_THRESHOLD	4 // excluding metadata (4 bytes)

class ChunkBuffer {
protected:
	LOCK_T lock;                           // Lock for the whole buffer

	static Coding *coding;                 // Coding module
	static MemoryPool<Chunk> *chunkPool;   // Memory pool for chunks
	static SlaveEventQueue *eventQueue;    // Event queue
	static Map *map;                       // Maps in slave

public:
	static uint32_t capacity;              // Chunk size
	static uint32_t dataChunkCount;        // Number of data chunks per stripe

	static void init();
	ChunkBuffer();
	virtual void print( FILE *f = stdout ) = 0;
	virtual void stop() = 0;
	virtual ~ChunkBuffer();
};

#endif

#ifndef __SLAVE_BUFFER_CHUNK_BUFFER_HH__
#define __SLAVE_BUFFER_CHUNK_BUFFER_HH__

#include <cstdio>
#include <pthread.h>
#include "../ds/map.hh"
#include "../event/event_queue.hh"
#include "../../common/ds/chunk.hh"
#include "../../common/ds/key_value.hh"
#include "../../common/ds/memory_pool.hh"
#include "../../common/ds/stripe.hh"

#define CHUNK_BUFFER_FLUSH_THRESHOLD	4 // excluding metadata (4 bytes)

class ChunkBuffer {
protected:
	uint32_t capacity;                     // Chunk size
	uint32_t count;                        // Number of chunks
	uint32_t listId;                       // List ID of this buffer
	uint32_t stripeId;                     // Current stripe ID
	uint32_t chunkId;                      // Chunk ID of this buffer
	pthread_mutex_t lock;                  // Lock for the whole buffer
	pthread_mutex_t *locks;                // Lock for each chunk
	Chunk **chunks;                        // Allocated chunk buffer
	static MemoryPool<Chunk> *chunkPool;   // Memory pool for chunks
	static MemoryPool<Stripe> *stripePool; // Memory pool for chunks
	static SlaveEventQueue *eventQueue;    // Event queue
	static Map *map;                       // Maps in slave

public:
	static void init();
	ChunkBuffer( uint32_t capacity, uint32_t count, uint32_t listId, uint32_t stripeId, uint32_t chunkId, bool isParity );
	virtual KeyMetadata set( char *key, uint8_t keySize, char *value, uint32_t valueSize, uint32_t chunkId = 0 ) = 0;
	virtual uint32_t flush( bool lock = true ) = 0;
	virtual Chunk *flush( int index, bool lock = true ) = 0;
	virtual void print( FILE *f = stdout ) = 0;
	virtual void stop() = 0;
	virtual ~ChunkBuffer();
};

#endif

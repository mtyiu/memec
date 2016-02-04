#ifndef __SLAVE_BUFFER_GET_CHUNK_BUFFER_HH__
#define __SLAVE_BUFFER_GET_CHUNK_BUFFER_HH__

#include <cstdio>
#include <pthread.h>
#include "../../common/ds/chunk.hh"
#include "../../common/ds/memory_pool.hh"
#include "../../common/lock/lock.hh"

struct GetChunkWrapper {
	Chunk *chunk;
	uint8_t sealIndicatorCount;
	bool *sealIndicator;
};

// Buffer for storing unmodified chunks for serving GET_CHUNK requests
class GetChunkBuffer {
protected:
	LOCK_T lock;
	std::unordered_map<Metadata, GetChunkWrapper> chunks;
	static MemoryPool<Chunk> *chunkPool;

public:
	static void init();
	GetChunkBuffer();
	virtual ~GetChunkBuffer();
	// bool insert( Metadata metadata, Chunk *chunk, bool needsLock = true, bool needsUnlock = true );
	bool insert( Metadata metadata, Chunk *chunk, uint8_t sealIndicatorCount = 0, bool *sealIndicator = 0, bool needsLock = true, bool needsUnlock = true );
	Chunk *find( Metadata metadata, bool &exists, uint8_t &sealIndicatorCount, bool *&sealIndicator, bool needsLock = true, bool needsUnlock = true );
	bool ack( Metadata metadata, bool needsLock = true, bool needsUnlock = true );
	bool erase( Metadata metadata, bool needsLock = true, bool needsUnlock = true );
	void unlock();
};

#endif

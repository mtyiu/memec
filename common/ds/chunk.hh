#ifndef __COMMON_DS_CHUNK_HH__
#define __COMMON_DS_CHUNK_HH__

#include <unordered_map>
#include <stdint.h>
#include <arpa/inet.h>
#include "metadata.hh"
#include "key.hh"
#include "key_value.hh"
#include "../lock/lock.hh"

#define USE_CHUNK_LOCK

enum ChunkStatus {
	// Clean chunk (used in chunk buffer)
	CHUNK_STATUS_EMPTY,
	// Some data is written and the chunk should be flushed later
	CHUNK_STATUS_DIRTY,
	// The chunk contains data that is already flushed to disk
	CHUNK_STATUS_CACHED,
	// The chunk is retrieved from surviving node using GET_CHUNK
	CHUNK_STATUS_FROM_GET_CHUNK,
	// Reconstructed using k other chunks
	CHUNK_STATUS_RECONSTRUCTED,
	// Temporary use
	CHUNK_STATUS_TEMPORARY
};

class Chunk {
private:
	char *data;                 // Buffer
	uint32_t size;              // Occupied data
#ifdef USE_CHUNK_LOCK
	LOCK_T lock;       // Lock
#endif

public:
	static uint32_t capacity;   // Chunk size
	ChunkStatus status;         // Current status of the chunk
	uint32_t count;             // Number of key-value pair
	Metadata metadata;          // Metadata (list ID, stripe ID, chunk ID)
	uint32_t lastDelPos;        // Record the position where the last key-value compaction ends after deletion
	bool isParity;              // Indicate whether the chunk is a parity chunk

	// Initialization
	Chunk();
	static void init( uint32_t capacity );
	void init();
	void swap( Chunk *c );
	void loadFromGetChunkRequest( uint32_t listId, uint32_t stripeId, uint32_t chunkId, bool isParity, char *data, uint32_t size );
	// Access data inside the chunk
	char *alloc( uint32_t size, uint32_t &offset );
#ifdef USE_CHUNK_LOCK
	int next( int offset, char *&key, uint8_t &keySize, bool needsLock = false, bool needsUnlock = false );
#else
	int next( int offset, char *&key, uint8_t &keySize );
#endif
	// Setters and getters
	inline char *getData() { return this->data; }
	inline uint32_t getSize() { return this->size; }
	inline void setData( char *data ) { this->data = data; }
	inline void setSize( uint32_t size ) { this->size = size; }
#ifdef USE_CHUNK_LOCK
	inline void setLock() { LOCK( &this->lock ); }
	inline void setUnlock() { UNLOCK( &this->lock ); }
#endif
	// Update internal counters for data and parity chunks
	uint32_t updateData();
	uint32_t updateParity( uint32_t offset = 0, uint32_t length = 0 );
	// Compute delta
	void computeDelta( char *delta, char *newData, uint32_t offset, uint32_t length, bool update = true );
	void update( char *newData, uint32_t offset, uint32_t length );
	// Delete key
	uint32_t deleteKeyValue( Key target, std::unordered_map<Key, KeyMetadata> *keys, char *delta = 0, size_t deltaBufSize = 0 );
	// Get key-value pair
	KeyValue getKeyValue( uint32_t offset );
	// Reset internal status
	void clear();
	void setReconstructed( uint32_t listId, uint32_t stripeId, uint32_t chunkId, bool isParity );
	void free();

	static bool initFn( Chunk *chunk, void *argv );
};

#endif

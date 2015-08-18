#ifndef __COMMON_DS_CHUNK_HH__
#define __COMMON_DS_CHUNK_HH__

#include <map>
#include <stdint.h>
#include <arpa/inet.h>
#include "metadata.hh"
#include "key.hh"
#include "key_value.hh"

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
public:
	static uint32_t capacity;   // Chunk size
	ChunkStatus status;         // Current status of the chunk
	uint32_t count;             // Number of key-value pair
	uint32_t size;              // Occupied data
	Metadata metadata;          // Metadata (list ID, stripe ID, chunk ID)
	bool isParity;              // Indicate whether the chunk is a parity chunk
	char *data;                 // Buffer

	// Initialization
	Chunk();
	static void init( uint32_t capacity );
	void init();
	char *alloc( uint32_t size, uint32_t &offset );
	void loadFromGetChunkRequest( uint32_t listId, uint32_t stripeId, uint32_t chunkId, bool isParity, char *data, uint32_t size );
	void swap( Chunk *c );
	// Update internal counters for data and parity chunks
	uint32_t updateData();
	uint32_t updateParity( uint32_t offset = 0, uint32_t length = 0 );
	// Compute delta
	void computeDelta( char *delta, char *newData, uint32_t offset, uint32_t length, bool update = true );
	// Delete key
	uint32_t deleteKeyValue( Key target, std::map<Key, KeyMetadata> *keys, char *delta, size_t deltaBufSize );
	// Get key-value pair
	KeyValue getKeyValue( uint32_t offset );
	// Reset internal status
	void clear();
	void setReconstructed( uint32_t listId, uint32_t stripeId, uint32_t chunkId, bool isParity );
	void free();

	static bool initFn( Chunk *chunk, void *argv );
};

#endif

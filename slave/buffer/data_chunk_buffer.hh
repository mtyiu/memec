#ifndef __SLAVE_BUFFER_DATA_CHUNK_BUFFER_HH__
#define __SLAVE_BUFFER_DATA_CHUNK_BUFFER_HH__

#include "chunk_buffer.hh"

class DataChunkBuffer : public ChunkBuffer {
private:
	pthread_mutex_t lock;     // Lock for sizes
	uint32_t *sizes;          // Occupied space for each chunk

public:
	DataChunkBuffer( uint32_t capacity, uint32_t count );
	KeyValue set( char *key, uint8_t keySize, char *value, uint32_t valueSize );
	size_t flush( bool lock = true );
	void stop();
	~DataChunkBuffer();
};

#endif

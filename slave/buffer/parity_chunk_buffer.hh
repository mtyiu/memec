#ifndef __SLAVE_BUFFER_PARITY_CHUNK_BUFFER_HH__
#define __SLAVE_BUFFER_PARITY_CHUNK_BUFFER_HH__

#include "chunk_buffer.hh"
#include "data_chunk_buffer.hh"

class ParityChunkBuffer : public ChunkBuffer {
private:
	uint32_t dataChunkCount;  // Number of data chunks per stripe
	DataChunkBuffer **dataChunkBuffer;

public:
	ParityChunkBuffer( uint32_t capacity, uint32_t count, uint32_t dataChunkCount );
	KeyValue set( char *key, uint8_t keySize, char *value, uint32_t valueSize, uint32_t dataIndex );
	size_t flush( bool lock = true );
	void stop();
	~ParityChunkBuffer();
};

#endif

#ifndef __SLAVE_BUFFER_MIXED_CHUNK_BUFFER_HH__
#define __SLAVE_BUFFER_MIXED_CHUNK_BUFFER_HH__

#include <cstdio>
#include "data_chunk_buffer.hh"
#include "parity_chunk_buffer.hh"

enum ChunkBufferRole {
	CBR_DATA,
	CBR_PARITY
};

class MixedChunkBuffer {
public:
	ChunkBufferRole role;
	union {
		DataChunkBuffer *data;
		ParityChunkBuffer *parity;
	} buffer;

	MixedChunkBuffer( DataChunkBuffer *dataChunkBuffer );
	MixedChunkBuffer( ParityChunkBuffer *parityChunkBuffer );
	KeyValue set( char *key, uint8_t keySize, char *value, uint32_t valueSize );
	uint32_t flush( bool lock = true );
	Chunk *flush( int index, bool lock = true );
	void print( FILE *f = stdout );
	void stop();
	~MixedChunkBuffer();
};

#endif

#include "chunk_buffer.hh"

ChunkBuffer::ChunkBuffer( MemoryPool<Chunk> *chunkPool, uint32_t capacity, uint32_t count ) {
	this->chunkPool = chunkPool;
	this->capacity = capacity;
	this->count = count;
	this->chunks = new Chunk*[ count ];
	this->locks = new pthread_mutex_t[ count ];
	chunkPool->malloc( this->chunks, this->count );
	for ( uint32_t i = 0; i < count; i++ )
		pthread_mutex_init( this->locks + i, 0 );
}

ChunkBuffer::~ChunkBuffer() {
	this->chunkPool->free( this->chunks, this->count );
	delete[] this->chunks;
	delete[] this->locks;
}

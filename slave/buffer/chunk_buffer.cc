#include "chunk_buffer.hh"

ChunkBuffer::ChunkBuffer( uint32_t capacity, uint32_t count ) {
	this->capacity = capacity;
	this->count = count;
	this->chunks = new Chunk[ count ];
	this->locks = new pthread_mutex_t[ count ];
	for ( uint32_t i = 0; i < count; i++ ) {
		this->chunks[ i ].init( capacity );
		pthread_mutex_init( this->locks + i, 0 );
	}
}

ChunkBuffer::~ChunkBuffer() {
	for ( size_t i = 0; i < this->count; i++ ) {
		this->chunks[ i ].free();
	}
	delete[] this->chunks;
	delete[] this->locks;
}

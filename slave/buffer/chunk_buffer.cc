#include "chunk_buffer.hh"

ChunkBuffer::ChunkBuffer( uint32_t capacity, uint32_t chunksPerList ) {
	this->capacity = capacity;
	this->count = chunksPerList;
	this->sizes = new uint32_t[ chunksPerList ];
	this->chunks = new Chunk[ chunksPerList ];
	this->locks = new pthread_mutex_t[ chunksPerList ];
	for ( uint32_t i = 0; i < chunksPerList; i++ ) {
		this->sizes[ i ] = 0;
		this->chunks[ i ].init( capacity );
		pthread_mutex_init( this->locks + i, 0 );
	}
}

ChunkBuffer::~ChunkBuffer() {
	for ( uint32_t i = 0; i < this->count; i++ ) {
		this->chunks[ i ].free();
	}
	delete this->sizes;
	delete this->chunks;
	delete this->locks;
}

void ChunkBuffer::stop() {

}

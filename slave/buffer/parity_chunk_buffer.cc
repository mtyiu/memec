#include "parity_chunk_buffer.hh"

ParityChunkBuffer::ParityChunkBuffer( uint32_t capacity, uint32_t count, uint32_t dataChunkCount, uint32_t listId, uint32_t stripeId, uint32_t chunkId ) : ChunkBuffer( capacity, count, listId, stripeId, chunkId ) {
	this->dataChunkCount = dataChunkCount;
	this->dataChunkBuffer = new DataChunkBuffer*[ dataChunkCount ];
	for ( uint32_t i = 0; i < dataChunkCount; i++ )
		this->dataChunkBuffer[ i ] = new DataChunkBuffer( capacity, count, listId, stripeId++, i );
}

KeyValue ParityChunkBuffer::set( char *key, uint8_t keySize, char *value, uint32_t valueSize ) {
	KeyValue ret;
	/*
	uint32_t size = 4 + keySize + valueSize, max = 0, tmp;
	int index = -1;

	pthread_mutex_lock( &this->lock );
	for ( uint32_t i = 0, j = 0; i < this->chunksPerList; i++ ) {
		j = dataIndex + i * this->dataChunkCount;
		tmp = this->sizes[ j ] + size;
		if ( tmp <= this->capacity ) {
			if ( tmp > max ) {
				max = tmp;
				index = j;
			}
		}
	}
	if ( index == -1 ) {
		index = this->flush( false );
	}
	pthread_mutex_unlock( &this->lock );
	*/

	return ret;
}

uint32_t ParityChunkBuffer::flush( bool lock ) {
	return 0;
}

Chunk *ParityChunkBuffer::flush( int index, bool lock ) {
	return 0;
}

void ParityChunkBuffer::print( FILE *f ) {

}

void ParityChunkBuffer::stop() {
}

ParityChunkBuffer::~ParityChunkBuffer() {
	for ( uint32_t i = 0; i < dataChunkCount; i++ ) {
		delete this->dataChunkBuffer[ i ];
	}
	delete[] this->dataChunkBuffer;
}

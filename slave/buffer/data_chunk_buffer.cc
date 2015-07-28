#include "data_chunk_buffer.hh"

DataChunkBuffer::DataChunkBuffer( uint32_t capacity, uint32_t count ) : ChunkBuffer( capacity, count ) {
	this->isParity = false;
	pthread_mutex_init( &this->lock, 0 );
	this->sizes = new uint32_t[ this->count ];
	for ( uint32_t i = 0; i < this->count; i++ ) {
		this->sizes[ i ] = 0;
	}
}

KeyValue DataChunkBuffer::set( char *key, uint8_t keySize, char *value, uint32_t valueSize ) {
	KeyValue ret;
	uint32_t size = 4 + keySize + valueSize, max = 0, tmp;
	int index = -1;

	pthread_mutex_lock( &this->lock );
	for ( size_t i = 0; i < this->count; i++ ) {
		tmp = this->sizes[ i ] + size;
		if ( tmp <= this->capacity ) {
			if ( tmp > max ) {
				max = tmp;
				index = i;
			}
		}
	}
	if ( index == -1 ) {
		index = this->flush( false );
	}
	pthread_mutex_unlock( &this->lock );

	return ret;
}

size_t DataChunkBuffer::flush( bool lock ) {
	if ( lock )
		pthread_mutex_lock( &this->lock );

	size_t index = 0;
	uint32_t max = this->sizes[ 0 ];

	for ( size_t i = 1; i < this->count; i++ ) {
		if ( this->sizes[ i ] > max ) {
			this->sizes[ i ] = max;
			index = i;
		}
	}

	// TODO: Seal operation

	if ( lock )
		pthread_mutex_unlock( &this->lock );

	return index;
}

void DataChunkBuffer::stop() {
}

DataChunkBuffer::~DataChunkBuffer() {
	delete[] this->sizes;
}

#include "data_chunk_buffer.hh"

DataChunkBuffer::DataChunkBuffer( MemoryPool<Chunk> *chunkPool, uint32_t capacity, uint32_t count ) : ChunkBuffer( chunkPool, capacity, count ) {
	pthread_mutex_init( &this->lock, 0 );
	this->sizes = new uint32_t[ this->count ];
	for ( uint32_t i = 0; i < this->count; i++ )
		this->sizes[ i ] = 0;
}

KeyValue DataChunkBuffer::set( char *key, uint8_t keySize, char *value, uint32_t valueSize ) {
	KeyValue keyValue;
	uint32_t size = 4 + keySize + valueSize, max = 0, tmp;
	int index = -1;

	// Choose one chunk buffer with minimum free space
	pthread_mutex_lock( &this->lock );
	for ( uint32_t i = 0; i < this->count; i++ ) {
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

	// Allocate memory in the selected chunk
	pthread_mutex_lock( this->locks + index );
	keyValue.data = this->chunks[ index ]->alloc( size );
	this->sizes[ index ] += size;

	if ( this->sizes[ index ] <= 4 + CHUNK_BUFFER_FLUSH_THRESHOLD ) {
		this->flush( index, false );
	}
	pthread_mutex_unlock( this->locks + index );
	pthread_mutex_unlock( &this->lock );

	// Copy data to the buffer
	keyValue.serialize( key, keySize, value, valueSize );

	return keyValue;
}

uint32_t DataChunkBuffer::flush( bool lock ) {
	if ( lock )
		pthread_mutex_lock( &this->lock );

	uint32_t index = 0;
	uint32_t max = this->sizes[ 0 ];

	for ( uint32_t i = 1; i < this->count; i++ ) {
		if ( this->sizes[ i ] > max ) {
			this->sizes[ i ] = max;
			index = i;
		}
	}

	// TODO: Seal operation
	Chunk *chunk = this->chunks[ index ];

	if ( lock )
		pthread_mutex_unlock( &this->lock );

	return index;
}

Chunk *DataChunkBuffer::flush( int index, bool lock ) {
	if ( lock )
		pthread_mutex_lock( &this->lock );

	Chunk *chunk = this->chunks[ index ];

	if ( lock )
		pthread_mutex_unlock( &this->lock );
}

void DataChunkBuffer::print( FILE *f ) {
	int width = 16;
	double occupied;
	fprintf(
		f,
		"- %-*s : %s\n"
		"- %-*s : %u\n"
		"- %-*s : %u\n"
		"- %-*s :\n",
		width, "Role", "Data chunk buffer",
		width, "Chunk size", this->capacity,
		width, "Number of chunks", this->count,
		width, "Statistics (occupied / total)"
	);
	for ( uint32_t i = 0; i < this->count; i++ ) {
		uint32_t size = this->sizes[ i ];
		occupied = ( double ) size / this->capacity;
		fprintf(
			f,
			"\t%u. %u / %u (%5.2lf%%)\n",
			( i + 1 ), size, this->capacity, occupied
		);
	}
}

void DataChunkBuffer::stop() {
}

DataChunkBuffer::~DataChunkBuffer() {
	delete[] this->sizes;
}

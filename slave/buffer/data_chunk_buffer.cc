#include "data_chunk_buffer.hh"
#include "../event/io_event.hh"

DataChunkBuffer::DataChunkBuffer( uint32_t capacity, uint32_t count, uint32_t listId, uint32_t stripeId, uint32_t chunkId, void ( *flushFn )( Chunk *, void * ), void *argv ) : ChunkBuffer( capacity, count, listId, stripeId, chunkId, false ) {
	this->flushFn = flushFn;
	this->argv = argv;
	this->sizes = new uint32_t[ this->count ];
	for ( uint32_t i = 0; i < this->count; i++ )
		this->sizes[ i ] = 0;
}

KeyMetadata DataChunkBuffer::set( char *key, uint8_t keySize, char *value, uint32_t valueSize, uint32_t chunkId ) {
	KeyMetadata keyMetadata;
	uint32_t size = 4 + keySize + valueSize, max = 0, tmp;
	int index = -1;
	char *ptr;

	// Choose one chunk buffer with minimum free space
	pthread_mutex_lock( &this->lock );
	for ( uint32_t i = 0; i < this->count; i++ ) {
		tmp = this->sizes[ i ] + size;
		if ( tmp <= this->capacity ) {
			if ( tmp > max ) {
				max = tmp;
				index = i;
			} else if ( tmp == max ) {
				if ( this->chunks[ i ]->metadata.stripeId < this->chunks[ index ]->metadata.stripeId )
					index = i;
			}
		}
	}
	if ( index == -1 ) {
		index = this->flush( false );
	}

	// Allocate memory in the selected chunk
	pthread_mutex_lock( this->locks + index );
	Chunk *chunk = this->chunks[ index ];

	// Set up key metadata
	keyMetadata.listId = chunk->metadata.listId;
	keyMetadata.stripeId = chunk->metadata.stripeId;
	keyMetadata.chunkId = chunk->metadata.chunkId;
	keyMetadata.length = size;

	// Allocate memory from chunk
	ptr = this->chunks[ index ]->alloc( size, keyMetadata.offset );
	this->sizes[ index ] += size;

	// Copy data to the buffer
	KeyValue::serialize( ptr, key, keySize, value, valueSize );

	// Flush if the current buffer is full
	if ( this->sizes[ index ] + 4 + CHUNK_BUFFER_FLUSH_THRESHOLD >= this->capacity ) {
		this->flush( index, false );
	}
	pthread_mutex_unlock( this->locks + index );
	pthread_mutex_unlock( &this->lock );

	return keyMetadata;
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
		} else if ( this->sizes[ i ] == max ) {
			if ( this->chunks[ i ]->metadata.stripeId < this->chunks[ index ]->metadata.stripeId )
					index = i;
		}
	}

	if ( lock )
		pthread_mutex_lock( this->locks + index );
	
	this->flush( index, false );

	if ( lock ) {
		pthread_mutex_unlock( this->locks + index );
		pthread_mutex_unlock( &this->lock );
	}

	return index;
}

Chunk *DataChunkBuffer::flush( int index, bool lock ) {
	if ( lock ) {
		pthread_mutex_lock( &this->lock );
		pthread_mutex_lock( this->locks + index );
	}
	
	Chunk *chunk = this->chunks[ index ];

	// Append a flush event to the event queue
	if ( ! this->flushFn ) {
		IOEvent ioEvent;
		ioEvent.flush( chunk );
		ChunkBuffer::eventQueue->insert( ioEvent );
	}

	// Get a new chunk
	this->sizes[ index ] = 0;
	Chunk *newChunk = ChunkBuffer::chunkPool->malloc();
	newChunk->clear();
	newChunk->metadata.listId = this->listId;
	newChunk->metadata.stripeId = this->stripeId;
	newChunk->metadata.chunkId = this->chunkId;
	newChunk->isParity = false;
	this->chunks[ index ] = newChunk;
	this->stripeId++;

	if ( lock ) {
		pthread_mutex_unlock( this->locks + index );
		pthread_mutex_unlock( &this->lock );
	}

	if ( this->flushFn ) {
		this->flushFn( chunk, this->argv );
	}

	return chunk;
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
		occupied = ( double ) size / this->capacity * 100.0;
		fprintf(
			f,
			"\t%u. [#%u] %u / %u (%5.2lf%%)\n",
			( i + 1 ), this->chunks[ i ]->metadata.stripeId, size, this->capacity, occupied
		);
	}
}

void DataChunkBuffer::stop() {
}

DataChunkBuffer::~DataChunkBuffer() {
	delete[] this->sizes;
}

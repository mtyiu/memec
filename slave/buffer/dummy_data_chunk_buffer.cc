#include "dummy_data_chunk_buffer.hh"

DummyDataChunk::DummyDataChunk() {
	this->status = CHUNK_STATUS_EMPTY;
	this->count = 0;
	this->size = 0;
}

void DummyDataChunk::alloc( uint32_t size, uint32_t &offset ) {
	offset = this->size;

	this->status = CHUNK_STATUS_DIRTY;
	this->count++;
	this->size += size;
}

void DummyDataChunk::clear() {
	this->status = CHUNK_STATUS_EMPTY;
	this->count = 0;
	this->size = 0;
}

///////////////////////////////////////////////////////////////////////////////

DummyDataChunkBuffer::DummyDataChunkBuffer( uint32_t count, uint32_t listId, uint32_t stripeId, uint32_t chunkId, void ( *flushFn )( uint32_t, void * ), void *argv ) {
	this->count = count;
	this->listId = listId;
	this->stripeId = stripeId;
	this->chunkId = chunkId;
	pthread_mutex_init( &this->lock, 0 );
	this->locks = new pthread_mutex_t[ count ];
	this->chunks = new DummyDataChunk*[ count ];
	this->sizes = new uint32_t[ count ];
	this->flushFn = flushFn;
	this->argv = argv;
	for ( uint32_t i = 0; i < count; i++ ) {
		pthread_mutex_init( this->locks + i, 0 );

		this->chunks[ i ] = new DummyDataChunk();

		Metadata &metadata = this->chunks[ i ]->metadata;
		metadata.listId = this->listId;
		metadata.stripeId = this->stripeId;
		metadata.chunkId = this->chunkId;

		this->sizes[ i ] = 0;

		this->stripeId++;
	}
}

uint32_t DummyDataChunkBuffer::set( uint32_t size, uint32_t &offset ) {
	uint32_t max = 0, tmp, ret;
	int index = -1;

	// Choose one chunk buffer with minimum free space
	pthread_mutex_lock( &this->lock );
	for ( uint32_t i = 0; i < this->count; i++ ) {
		tmp = this->sizes[ i ] + size;
		if ( tmp <= ChunkBuffer::capacity ) {
			if ( tmp > max ) {
				max = tmp;
				index = i;
			} else if ( tmp == max ) {
				if ( this->chunks[ i ]->metadata.stripeId < this->chunks[ index ]->metadata.stripeId )
					index = i;
			}
		}
	}
	if ( index == -1 )
		index = this->flush( false );

	// Allocate memory in the selected chunk
	pthread_mutex_lock( this->locks + index );
	DummyDataChunk *chunk = this->chunks[ index ];

	ret = chunk->metadata.stripeId;

	// Allocate memory from chunk
	this->chunks[ index ]->alloc( size, offset );
	this->sizes[ index ] += size;

	// Flush if the current buffer is full
	if ( this->sizes[ index ] + 4 + CHUNK_BUFFER_FLUSH_THRESHOLD >= ChunkBuffer::capacity )
		this->flush( index, false );
	pthread_mutex_unlock( this->locks + index );
	pthread_mutex_unlock( &this->lock );

	return ret; // the ID of the selected stripe
}

uint32_t DummyDataChunkBuffer::flush( bool lock ) {
	if ( lock )
		pthread_mutex_lock( &this->lock );

	uint32_t index = 0;
	uint32_t max = this->sizes[ 0 ];

	for ( uint32_t i = 1; i < this->count; i++ ) {
		if ( this->sizes[ i ] > max ) {
			max = this->sizes[ i ];
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

uint32_t DummyDataChunkBuffer::flush( int index, bool lock ) {
	if ( lock ) {
		pthread_mutex_lock( &this->lock );
		pthread_mutex_lock( this->locks + index );
	}

	DummyDataChunk *chunk = this->chunks[ index ];
	uint32_t ret = chunk->metadata.stripeId;

	this->flushFn( ret, this->argv );

	// Reset the dummy data chunk
	this->sizes[ index ] = 0;
	chunk->clear();
	chunk->metadata.listId = this->listId;
	chunk->metadata.stripeId = this->stripeId;
	chunk->metadata.chunkId = this->chunkId;
	this->stripeId++;

	if ( lock ) {
		pthread_mutex_unlock( this->locks + index );
		pthread_mutex_unlock( &this->lock );
	}

	return ret;
}

void DummyDataChunkBuffer::print( FILE *f ) {
   int width = 16;
	double occupied;
	fprintf(
		f,
		"- %-*s : %s\n"
		"- %-*s : %u\n"
		"- %-*s : %u\n"
		"- %-*s :\n",
		width, "Role", "Dummy Data chunk buffer",
		width, "Chunk size", ChunkBuffer::capacity,
		width, "Number of chunks", this->count,
		width, "Statistics (occupied / total)"
	);
	for ( uint32_t i = 0; i < this->count; i++ ) {
		uint32_t size = this->sizes[ i ];
		occupied = ( double ) size / ChunkBuffer::capacity * 100.0;
		fprintf(
			f,
			"\t%u. [#%u] %u / %u (%5.2lf%%)\n",
			( i + 1 ), this->chunks[ i ]->metadata.stripeId, size, ChunkBuffer::capacity, occupied
		);
	}
}

DummyDataChunkBuffer::~DummyDataChunkBuffer() {
	for ( uint32_t i = 0; i < this->count; i++ )
		delete this->chunks[ i ];
	delete[] this->locks;
	delete[] this->chunks;
	delete[] this->sizes;
}

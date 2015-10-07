#include "data_chunk_buffer.hh"
#include "../main/slave.hh"
#include "../worker/worker.hh"

DataChunkBuffer::DataChunkBuffer( uint32_t count, uint32_t listId, uint32_t stripeId, uint32_t chunkId ) : ChunkBuffer( listId, stripeId, chunkId ) {
	this->count = count;
	this->locks = new pthread_mutex_t[ count ];
	this->chunks = new Chunk*[ count ];
	this->sizes = new uint32_t[ count ];

	ChunkBuffer::chunkPool->malloc( this->chunks, this->count );
	for ( uint32_t i = 0; i < count; i++ ) {
		pthread_mutex_init( this->locks + i, 0 );

		Metadata &metadata = this->chunks[ i ]->metadata;
		metadata.listId = this->listId;
		metadata.stripeId = this->stripeId;
		metadata.chunkId = this->chunkId;
		// ChunkBuffer::map->cache[ metadata ] = this->chunks[ i ];
		ChunkBuffer::map->setChunk( this->listId, this->stripeId, this->chunkId, this->chunks[ i ], false );

		this->sizes[ i ] = 0;

		this->stripeId++;
	}
}

KeyMetadata DataChunkBuffer::set( SlaveWorker *worker, char *key, uint8_t keySize, char *value, uint32_t valueSize, uint8_t opcode ) {
	KeyMetadata keyMetadata;
	uint32_t size = PROTO_KEY_VALUE_SIZE + keySize + valueSize, max = 0, tmp;
	int index = -1;
	char *ptr;

	// Choose one chunk buffer with minimum free space
	pthread_mutex_lock( &this->lock );
	for ( uint32_t i = 0; i < this->count; i++ ) {
		tmp = this->sizes[ i ] + size;
		if ( tmp <= ChunkBuffer::capacity ) {
			if ( tmp > max ) {
				max = tmp;
				index = i;
			} else if ( tmp == max && index != -1 ) {
				if ( this->chunks[ i ]->metadata.stripeId < this->chunks[ index ]->metadata.stripeId )
					index = i;
			}
		}
	}
	if ( index == -1 )
		index = this->flush( worker, false, true );

	// Allocate memory in the selected chunk
	pthread_mutex_lock( this->locks + index );
	Chunk *chunk = this->chunks[ index ];

	// Set up key metadata
	keyMetadata.listId = chunk->metadata.listId;
	keyMetadata.stripeId = chunk->metadata.stripeId;
	keyMetadata.chunkId = chunk->metadata.chunkId;
	keyMetadata.length = size;

	// Allocate memory from chunk
	assert( this->sizes[ index ] == this->chunks[ index ]->getSize() );
	ptr = this->chunks[ index ]->alloc( size, keyMetadata.offset );
	this->sizes[ index ] += size;

	// Copy data to the buffer
	KeyValue::serialize( ptr, key, keySize, value, valueSize );

	// Flush if the current buffer is full
	if ( this->sizes[ index ] + PROTO_KEY_VALUE_SIZE + CHUNK_BUFFER_FLUSH_THRESHOLD >= ChunkBuffer::capacity )
		this->flushAt( worker, index, false );

	pthread_mutex_unlock( this->locks + index );
	pthread_mutex_unlock( &this->lock );

	// Update key map
	Key keyObj;
	keyObj.set( keySize, key );
	ChunkBuffer::map->insertKey( keyObj, opcode, keyMetadata );

	return keyMetadata;
}

size_t DataChunkBuffer::seal( SlaveWorker *worker ) {
	pthread_mutex_lock( &this->lock );
	for ( uint32_t i = 0; i < this->count; i++ ) {
		this->flushAt( worker, i, false );
	}
	pthread_mutex_unlock( &this->lock );
	return this->count;
}

int DataChunkBuffer::lockChunk( Chunk *chunk ) {
	int index = -1;
	pthread_mutex_lock( &this->lock );
	for ( uint32_t i = 0; i < this->count; i++ ) {
		if ( this->chunks[ i ] == chunk ) {
			index = i;
			break;
		}
	}
	if ( index != -1 ) {
		// Found
		pthread_mutex_lock( this->locks + index );
	} else {
		pthread_mutex_unlock( &this->lock );
	}
	return index;
}

void DataChunkBuffer::updateAndUnlockChunk( int index ) {
	this->sizes[ index ] = this->chunks[ index ]->getSize();
	pthread_mutex_unlock( this->locks + index );
	pthread_mutex_unlock( &this->lock );
}

uint32_t DataChunkBuffer::flush( SlaveWorker *worker, bool lock, bool lockAtIndex ) {
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

	if ( lock || lockAtIndex )
		pthread_mutex_lock( this->locks + index );

	this->flushAt( worker, index, false );

	if ( lock || lockAtIndex )
		pthread_mutex_unlock( this->locks + index );

	if ( lock )
		pthread_mutex_unlock( &this->lock );

	return index;
}

Chunk *DataChunkBuffer::flushAt( SlaveWorker *worker, int index, bool lock ) {
	if ( lock ) {
		pthread_mutex_lock( &this->lock );
		pthread_mutex_lock( this->locks + index );
	}

	Chunk *chunk = this->chunks[ index ];

	// Get a new chunk
	this->sizes[ index ] = 0;
	Chunk *newChunk = ChunkBuffer::chunkPool->malloc();
	newChunk->clear();
	newChunk->metadata.listId = this->listId;
	newChunk->metadata.stripeId = this->stripeId;
	newChunk->metadata.chunkId = this->chunkId;
	newChunk->isParity = false;
	// ChunkBuffer::map->cache[ newChunk->metadata ] = newChunk;
	ChunkBuffer::map->setChunk( this->listId, this->stripeId, this->chunkId, newChunk, false );
	this->chunks[ index ] = newChunk;
	this->stripeId++;

	// Notify the parity slaves to seal the chunk
	worker->issueSealChunkRequest( chunk );

	if ( lock ) {
		pthread_mutex_unlock( this->locks + index );
		pthread_mutex_unlock( &this->lock );
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

void DataChunkBuffer::stop() {}

DataChunkBuffer::~DataChunkBuffer() {
	this->chunkPool->free( this->chunks, this->count );
	delete[] this->locks;
	delete[] this->chunks;
	delete[] this->sizes;
}

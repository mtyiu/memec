#include "parity_chunk_buffer.hh"
#include "../../common/util/debug.hh"

ParityChunkBuffer::ParityChunkBuffer( uint32_t capacity, uint32_t count, uint32_t dataChunkCount, uint32_t listId, uint32_t stripeId, uint32_t chunkId ) : ChunkBuffer( capacity, count, listId, stripeId, chunkId, true ) {

	this->dataChunkCount = dataChunkCount;
	this->dataChunks = new Chunk**[ count ];
	this->dataChunkBuffer = new DataChunkBuffer*[ dataChunkCount ];
	for ( uint32_t i = 0; i < count; i++ ) {
		this->dataChunks[ i ] = new Chunk*[ dataChunkCount ];
	}
	for ( uint32_t i = 0; i < dataChunkCount; i++ ) {
		this->dataChunkBuffer[ i ] = new DataChunkBuffer( capacity, count, listId, stripeId, i, ParityChunkBuffer::dataChunkFlushHandler, ( void * ) this );
	}
	this->status = new BitmaskArray( dataChunkCount, count );
}

ParityChunkBuffer::~ParityChunkBuffer() {
	for ( uint32_t i = 0; i < this->count; i++ ) {
		delete[] this->dataChunks[ i ];
	}
	for ( uint32_t i = 0; i < this->dataChunkCount; i++ ) {
		delete[] this->dataChunkBuffer[ i ];
	}
	delete[] this->dataChunks;
	delete[] this->dataChunkBuffer;
	delete this->status;
}

KeyValue ParityChunkBuffer::set( char *key, uint8_t keySize, char *value, uint32_t valueSize, uint32_t chunkId ) {
	return this->dataChunkBuffer[ chunkId ]->set( key, keySize, value, valueSize );
}

uint32_t ParityChunkBuffer::flush( bool lock ) {
	if ( lock )
		pthread_mutex_lock( &this->lock );

	uint32_t index = 0, max = 0, count;

	for ( uint32_t i = 0; i < this->count; i++ ) {
		count = 0;
		for ( uint32_t j = 0; j < this->dataChunkCount; j++ ) {
			if ( this->status->check( i, j ) )
				count++;
		}
		if ( count > max ) {
			index = i;
			max = count;
		} else if ( count == max ) {
			if ( this->chunks[ i ]->stripeId < this->chunks[ index ]->stripeId )
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

Chunk *ParityChunkBuffer::flush( int index, bool lock ) {
	if ( lock ) {
		pthread_mutex_lock( &this->lock );
		pthread_mutex_lock( this->locks + index );
	}
	
	Chunk *chunk = this->chunks[ index ];
	Stripe *stripe = ChunkBuffer::stripePool->malloc();
	stripe->set( this->dataChunks[ index ], chunk, this->chunkId );

	// Append an encode event to the event queue
	CodingEvent codingEvent;
	codingEvent.encode( stripe );
	ChunkBuffer::eventQueue->insert( codingEvent );

	// Get a new chunk
	for ( uint32_t i = 0; i < this->dataChunkCount; i++ )
		this->dataChunks[ index ][ i ] = 0;
	this->status->clear( index );
	this->chunks[ index ] = ChunkBuffer::chunkPool->malloc();
	this->chunks[ index ]->clear();
	this->chunks[ index ]->listId = this->listId;
	this->chunks[ index ]->stripeId = this->stripeId;
	this->chunks[ index ]->chunkId = this->chunkId;
	this->stripeId++;

	if ( lock ) {
		pthread_mutex_unlock( &this->lock );
		pthread_mutex_unlock( this->locks + index );
	}

	return chunk;
}

void ParityChunkBuffer::print( FILE *f ) {
	int width = 16;
	fprintf(
		f,
		"- %-*s : %s\n"
		"- %-*s : %u\n"
		"- %-*s\n",
		width, "Role", "Data chunk buffer",
		width, "Chunk size", this->capacity,
		width, "Bitmap"
	);
	this->status->print( f );
	for ( uint32_t i = 0; i < this->dataChunkCount; i++ ) {
		fprintf( f, "\n*** Data chunk #%u ***\n", i );
		this->dataChunkBuffer[ i ]->print( f );
	}
}

void ParityChunkBuffer::stop() {
}

void ParityChunkBuffer::flushData( Chunk *chunk ) {
	int index = -1;

	pthread_mutex_lock( &this->lock );

	// Find the stripe index
	for ( uint32_t i = 0; i < this->count; i++ ) {
		if ( this->chunks[ i ]->listId == chunk->listId &&
		     this->chunks[ i ]->stripeId == chunk->stripeId ) {
			index = i;
			break;
		}
	}

	if ( index == -1 ) {
		__ERROR__( "ParityChunkBuffer", "flushData", "Cannot find the stripe in parity chunk buffer." );
		pthread_mutex_unlock( &this->lock );
		return;
	}

	pthread_mutex_lock( this->locks + index );
	// Store the data chunks into temporary buffer
	this->dataChunks[ index ][ chunk->chunkId ] = chunk;
	this->status->set( index, chunk->chunkId );

	// Check whether a chunk is ready to be flushed
	if ( this->status->checkAllSet( index ) )
		this->flush( index, false );
	pthread_mutex_unlock( this->locks + index );

	pthread_mutex_unlock( &this->lock );
}

Chunk *ParityChunkBuffer::flushData( uint32_t chunkId, int index, bool lock ) {
	return this->dataChunkBuffer[ chunkId ]->flush( index, true );
}

void ParityChunkBuffer::dataChunkFlushHandler( Chunk *chunk, void *argv ) {
	( ( ParityChunkBuffer * ) argv )->flushData( chunk );
}

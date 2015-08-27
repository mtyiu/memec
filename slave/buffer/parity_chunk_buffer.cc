#include "parity_chunk_buffer.hh"
#include "../../common/util/debug.hh"

ParityChunkWrapper::ParityChunkWrapper() {
	this->pending = ChunkBuffer::dataChunkCount;
	pthread_mutex_init( &this->lock, 0 );
	this->chunk = 0;
}

///////////////////////////////////////////////////////////////////////////////

ParityChunkBuffer::ParityChunkBuffer( uint32_t count, uint32_t listId, uint32_t stripeId, uint32_t chunkId ) : ChunkBuffer( listId, stripeId, chunkId ) {
	this->dummyDataChunkBuffer = new DummyDataChunkBuffer*[ ChunkBuffer::dataChunkCount ];
	for ( uint32_t i = 0; i < ChunkBuffer::dataChunkCount; i++ ) {
		this->dummyDataChunkBuffer[ i ] = new DummyDataChunkBuffer( count, listId, stripeId, chunkId, ParityChunkBuffer::dataChunkFlushHandler, this );
	}
}

ParityChunkWrapper &ParityChunkBuffer::getWrapper( uint32_t stripeId ) {
	pthread_mutex_lock( &this->lock );
	std::map<uint32_t, ParityChunkWrapper>::iterator it = this->chunks.find( stripeId );
	if ( it == this->chunks.end() ) {
		ParityChunkWrapper wrapper;
		wrapper.chunk = ChunkBuffer::chunkPool->malloc();
		this->chunks[ stripeId ] = wrapper;
		it = this->chunks.find( stripeId );
	}
	ParityChunkWrapper &wrapper = it->second;
	pthread_mutex_unlock( &this->lock );
	return wrapper;
}

void ParityChunkBuffer::set( char *key, uint8_t keySize, char *value, uint32_t valueSize, uint32_t chunkId ) {
	uint32_t offset, size = 4 + keySize + valueSize;
	uint32_t stripeId = this->dummyDataChunkBuffer[ chunkId ]->set( size, offset );
	ParityChunkWrapper &wrapper = this->getWrapper( stripeId );

	pthread_mutex_lock( &wrapper.lock );

	// Compute parity delta

	// Update the parity chunk

	pthread_mutex_unlock( &wrapper.lock );
}

void ParityChunkBuffer::flush( uint32_t stripeId, Chunk *chunk ) {
	// Append a flush event to the event queue
	IOEvent ioEvent;
	ioEvent.flush( chunk );
	ChunkBuffer::eventQueue->insert( ioEvent );
}

void ParityChunkBuffer::print( FILE *f ) {
}

void ParityChunkBuffer::stop() {}

ParityChunkBuffer::~ParityChunkBuffer() {
	for ( uint32_t i = 0; i < ChunkBuffer::dataChunkCount; i++ )
		delete this->dummyDataChunkBuffer[ i ];
	delete[] this->dummyDataChunkBuffer;
}

void ParityChunkBuffer::flushData( uint32_t stripeId ) {
	ParityChunkWrapper &wrapper = this->getWrapper( stripeId );

	pthread_mutex_lock( &wrapper.lock );
	wrapper.pending--;
	if ( wrapper.pending == 0 )
		this->flush( stripeId, wrapper.chunk );
	pthread_mutex_unlock( &wrapper.lock );
}

void ParityChunkBuffer::dataChunkFlushHandler( uint32_t stripeId, void *argv ) {
	( ( ParityChunkBuffer * ) argv )->flushData( stripeId );
}

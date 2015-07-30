#include "chunk_buffer.hh"

MemoryPool<Chunk> *ChunkBuffer::chunkPool;
MemoryPool<Stripe> *ChunkBuffer::stripePool;
SlaveEventQueue *ChunkBuffer::eventQueue;

void ChunkBuffer::init( MemoryPool<Chunk> *chunkPool, MemoryPool<Stripe> *stripePool, SlaveEventQueue *eventQueue ) {
	ChunkBuffer::chunkPool = chunkPool;
	ChunkBuffer::stripePool = stripePool;
	ChunkBuffer::eventQueue = eventQueue;
}

ChunkBuffer::ChunkBuffer( uint32_t capacity, uint32_t count, uint32_t listId, uint32_t stripeId, uint32_t chunkId, bool isParity ) {
	this->capacity = capacity;
	this->count = count;
	this->listId = listId;
	this->stripeId = stripeId;
	this->chunkId = chunkId;
	pthread_mutex_init( &this->lock, 0 );
	this->locks = new pthread_mutex_t[ count ];
	this->chunks = new Chunk*[ count ];
	ChunkBuffer::chunkPool->malloc( this->chunks, this->count );
	for ( uint32_t i = 0; i < count; i++ ) {
		this->chunks[ i ]->listId = this->listId;
		this->chunks[ i ]->stripeId = this->stripeId;
		this->chunks[ i ]->chunkId = this->chunkId;
		this->chunks[ i ]->isParity = isParity;
		pthread_mutex_init( this->locks + i, 0 );
		this->stripeId++;
	}
}

ChunkBuffer::~ChunkBuffer() {
	this->chunkPool->free( this->chunks, this->count );
	delete[] this->chunks;
	delete[] this->locks;
}

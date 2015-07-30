#include "stripe.hh"

uint32_t Stripe::dataChunkCount;
uint32_t Stripe::parityChunkCount;

Stripe::Stripe() {
	this->chunks = new Chunk*[ Stripe::dataChunkCount + Stripe::parityChunkCount ];
	for ( uint32_t i = 0; i < Stripe::dataChunkCount + Stripe::parityChunkCount; i++ )
		this->chunks[ i ] = 0;
}

Stripe::~Stripe() {
	delete[] this->chunks;
}

void Stripe::set( Chunk **dataChunks, Chunk *parityChunk, uint32_t parityChunkId ) {
	for ( uint32_t i = 0; i < Stripe::dataChunkCount; i++ )
		this->chunks[ i ] = dataChunks[ i ];
	for ( uint32_t i = 0; i < Stripe::parityChunkCount; i++ )
		this->chunks[ Stripe::dataChunkCount + i ] = 0;
	this->chunks[ parityChunkId ] = parityChunk;
}

void Stripe::set( Chunk **dataChunks, Chunk **parityChunks ) {
	for ( uint32_t i = 0; i < Stripe::dataChunkCount; i++ )
		this->chunks[ i ] = dataChunks[ i ];
	for ( uint32_t i = 0; i < Stripe::parityChunkCount; i++ )
		this->chunks[ Stripe::dataChunkCount + i ] = parityChunks[ i ];
}

void Stripe::init( uint32_t dataChunkCount, uint32_t parityChunkCount ) {
	Stripe::dataChunkCount = dataChunkCount;
	Stripe::parityChunkCount = parityChunkCount;
}

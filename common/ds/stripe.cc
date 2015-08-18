#include "stripe.hh"
#include "../coding/coding.hh"
#include "../util/debug.hh"

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
	for ( uint32_t i = 0; i < Stripe::dataChunkCount; i++ ) {
		this->chunks[ i ] = dataChunks[ i ];
	}
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

void Stripe::set( Chunk *dataChunk, uint32_t dataChunkId, Chunk *parityChunk, uint32_t parityChunkId ) {
	Chunk *zeros = Coding::zeros;
	for ( uint32_t i = 0; i < Stripe::dataChunkCount; i++ )
		this->chunks[ i ] = i == dataChunkId ? dataChunk : zeros;
	for ( uint32_t i = 0; i < Stripe::parityChunkCount; i++ )
		this->chunks[ Stripe::dataChunkCount + i ] =
			( Stripe::dataChunkCount + i == parityChunkId )
			? parityChunk : zeros;
}

void Stripe::get( Chunk **&dataChunks, Chunk **&parityChunks ) {
	dataChunks = this->chunks;
	parityChunks = this->chunks + Stripe::dataChunkCount;
}

void Stripe::get( Chunk **&dataChunks, Chunk *&parityChunk, uint32_t parityChunkId ) {
	dataChunks = this->chunks;
	parityChunk = this->chunks[ parityChunkId ];
}

uint32_t Stripe::get( Chunk **&dataChunks, Chunk *&parityChunk ) {
	uint32_t parityChunkId, chunkCount = Stripe::dataChunkCount + Stripe::parityChunkCount;
	for ( parityChunkId = Stripe::dataChunkCount; parityChunkId < chunkCount; parityChunkId++ ) {
		if ( this->chunks[ parityChunkId ] ) {
			this->get( dataChunks, parityChunk, parityChunkId );
			return ( parityChunkId - Stripe::dataChunkCount + 1 );
		}
	}
	__ERROR__( "Stripe", "get", "Cannot find allocated parity chunk." );
	return 0;
}

uint32_t Stripe::getMaxDataChunkSize() {
	uint32_t ret = 0;
	for ( uint32_t i = 0; i < Stripe::dataChunkCount; i++ ) {
		if ( ret < this->chunks[ i ]->size )
			ret = this->chunks[ i ]->size;
	}
	return ret;
}

void Stripe::init( uint32_t dataChunkCount, uint32_t parityChunkCount ) {
	Stripe::dataChunkCount = dataChunkCount;
	Stripe::parityChunkCount = parityChunkCount;
}

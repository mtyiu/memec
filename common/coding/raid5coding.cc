#ifndef __COMMON_CODING_RAID5CODING_HH__
#define __COMMON_CODING_RAID5CODING_HH__

#include "raid5coding.hh"

bool RAID5Coding::init( uint32_t n ) {
	this->n = n;
}

void RAID5Coding::encode( Chunk **data, Chunk *parity, uint32_t index ) {
	for ( uint32_t i = 0; i < this->n - 1; i++ ) {
		this->bitwiseXOR( parity->data, parity->data, data[ i ]->data, data[ i ]->size );
		if ( data[ i ]->size > parity->size )
			parity->size = data[ i ]->size;
	}
}

bool RAID5Coding::decode( Chunk **chunks, BitmaskArray *bitmap ) {
	uint32_t lostIndex;

	// Check which chunk is lost
	for ( uint32_t i = 0; i < this->n; i++ ) {
		if ( ! this->bitmap->get( i ) ) {
			lostIndex = i;
			break;
		}
	}
	
	// Check whether there are more lost chunks
	for ( uint32_t i = lostIndex + 1; i < this->n; i++ )
		if ( ! this->bitmap->get( i ) )
			return false;

	// Reconstruct the lost chunk
	Chunk *lostChunk = this->chunks[ lostIndex ];
	for ( uint32_t i = 0; i < this->n; i++ ) {
		if ( i == lostChunk )
			continue;
		this->bitwiseXOR( lostChunk, lostChunk, chunks[ i ]->data, chunks[ i ]->size );
	}

	// Update chunk's internal counter
	lostChunk->update( lostIndex == this->n - 1 );
}

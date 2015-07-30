#include "raid5coding.hh"

bool RAID5Coding::init( uint32_t n ) {
	this->n = n;
	return true;
}

void RAID5Coding::encode( Chunk **data, Chunk *parity, uint32_t index ) {
	for ( uint32_t i = 0; i < this->n - 1; i++ )
		this->bitwiseXOR( parity, parity, data[ i ], data[ i ]->size );
}

bool RAID5Coding::decode( Chunk **chunks, BitmaskArray *bitmap ) {
	uint32_t lostIndex = 0;

	// Check which chunk is lost
	for ( uint32_t i = 0; i < this->n; i++ ) {
		if ( ! bitmap->check( i ) ) {
			lostIndex = i;
			break;
		}
	}
	
	// Check whether there are more lost chunks
	for ( uint32_t i = lostIndex + 1; i < this->n; i++ )
		if ( ! bitmap->check( i ) )
			return false;

	// Reconstruct the lost chunk
	Chunk *lostChunk = chunks[ lostIndex ];
	for ( uint32_t i = 0; i < this->n; i++ ) {
		if ( i == lostIndex )
			continue;
		this->bitwiseXOR( lostChunk, lostChunk, chunks[ i ], chunks[ i ]->size );
	}

	// Update chunk's internal counter
	if ( lostIndex == this->n - 1 )
		lostChunk->updateParity();
	else
		lostChunk->updateData();

	return true;
}

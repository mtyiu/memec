#include "raid1coding.hh"

bool RAID1Coding::init( uint32_t n ) {
	this->n = n;
	return true;
}

void RAID1Coding::encode( Chunk **data, Chunk *parity, uint32_t index, uint32_t startOff, uint32_t endOff ) {
	memcpy( parity->data, data[ 0 ]->data, data[ 0 ]->size );
}

bool RAID1Coding::decode( Chunk **chunks, BitmaskArray *bitmap ) {
	uint32_t survivingIndex = 0;
	bool ret = false;

	// Check which chunk is lost
	for ( uint32_t i = 0; i < this->n; i++ ) {
		if ( bitmap->check( i ) ) {
			survivingIndex = i;
			ret = true;
			break;
		}
	}

	if ( ! ret ) return false; // No surviving chunks remained

	// Reconstruct the lost chunk(s)
	for ( uint32_t i = 0; i < this->n; i++ ) {
		if ( ! bitmap->check( i ) ) {
			memcpy( chunks[ i ]->data, chunks[ survivingIndex ]->data, chunks[ survivingIndex ]->size );
		}
	}

	return true;
}

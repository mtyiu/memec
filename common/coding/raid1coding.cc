#include "raid1coding.hh"

bool RAID1Coding::init( uint32_t n ) {
	this->n = n;
	return true;
}

void RAID1Coding::encode( Chunk **data, Chunk *parity, uint32_t index, uint32_t startOff, uint32_t endOff ) {
	memcpy( parity->getData(), data[ 0 ]->getData(), data[ 0 ]->getSize() );
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
			memcpy( chunks[ i ]->getData(), chunks[ survivingIndex ]->getData(), chunks[ survivingIndex ]->getSize() );
		}
	}

	return true;
}

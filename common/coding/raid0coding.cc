#include "raid0coding.hh"

bool RAID0Coding::init( uint32_t n ) {
	this->n = n;
	return true;
}

void RAID0Coding::encode( Chunk **data, Chunk *parity, uint32_t index, uint32_t startOff, uint32_t endOff ) {
	// No parity chunk to encode
}

bool RAID0Coding::decode( Chunk **chunks, BitmaskArray *bitmap ) {
	// Check whether any chunk is lost
	for ( uint32_t i = 0; i < this->n; i++ ) {
		if ( ! bitmap->check( i ) ) {
			return false;
		}
	}
	return true;
}

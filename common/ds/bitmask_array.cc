#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "bitmask_array.hh"

BitmaskArray::BitmaskArray( size_t size, size_t count ) {
	this->size = size;
	this->count = count;
	this->total = size * count;
	if ( this->total & 63 ) {
		this->total >>= 6;
		this->total++;
	} else {
		this->total >>= 6;
	}
	this->bitmasks = ( uint64_t * ) malloc( sizeof( uint64_t ) * total );
	memset( this->bitmasks, 0, sizeof( uint64_t ) * total );
}

BitmaskArray::~BitmaskArray() {
	free( this->bitmasks );
}

void BitmaskArray::set( size_t entry, size_t bit ) {
	size_t index = entry * this->size + bit;
	uint64_t *bitmask = this->bitmasks + ( index >> 6 );
	*bitmask |= ( 1l << ( index & 63 ) );
}

void BitmaskArray::unset( size_t entry, size_t bit ) {
	size_t index = entry * this->size + bit;
	uint64_t *bitmask = this->bitmasks + ( index >> 6 );
	*bitmask &= ~( 1l << ( index & 63 ) );
}

bool BitmaskArray::check( size_t entry, size_t bit ) {
	size_t index = entry * this->size + bit;
	uint64_t *bitmask = this->bitmasks + ( index >> 6 );
	return ( *bitmask & ( 1l << ( index & 63 ) ) );
}

void BitmaskArray::print() {
	int entry, bit, index;
	uint64_t *bitmask;

	index = 0;
	for ( entry = 0; entry < this->count; entry++ ) {
		printf( "[%5d]", entry );
		for ( bit = 0; bit < this->size; bit++, index++ ) {
			bitmask = this->bitmasks + ( index >> 6 );
			printf( " %c", ( *bitmask & ( 1l << ( index & 63 ) ) ) ? '1' : '0' );
		}
		printf( "\n" );
	}
}

void BitmaskArray::printRaw() {
	size_t i;
	for ( i = 0; i < this->total; i++ ) {
		printf( "[%5lu] %lu\n", i, this->bitmasks[ i ] );
	}
}

void BitmaskArray::clear( size_t entry ) {
	size_t bit, index;
	uint64_t *bitmask;
	for ( bit = 0; bit < this->size; bit++ ) {
		index = entry * this->size + bit;
		bitmask = this->bitmasks + ( index >> 6 );
		if ( ( index & 63 ) == 0 && bit + 64 <= this->size ) {
			*bitmask = 0;
			bit += 63;
		} else {
			*bitmask &= ~( 1l << ( index & 63 ) );
		}
	}
}
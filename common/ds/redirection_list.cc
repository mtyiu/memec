#include "redirection_list.hh"

RedirectionList::RedirectionList() {
	this->original = 0;
	this->redirected = 0;
	this->count = 0;
}

void RedirectionList::init( uint8_t count ) {
	this->original = new uint8_t[ count * 2 ];
	this->redirected = this->original + count;
	this->count = count;
}

void RedirectionList::free() {
	if ( this->original ) delete[] this->original;
}

void RedirectionList::set( uint8_t *original, uint8_t *redirected, uint8_t count, bool copy ) {
	this->count = count;

	if ( ! count ) {
		this->original = 0;
		this->redirected = 0;
		return;
	}

	if ( copy ) {
		this->init( count );
		for ( uint8_t i = 0; i < count; i++ ) {
			this->original[ i ] = original[ i ];
			this->redirected[ i ] = redirected[ i ];
		}
	} else {
		this->original = original;
		this->redirected = redirected;
	}
}

void RedirectionList::print( FILE *f ) {
	if ( ! this->original || ! this->redirected || ! this->count ) return;
	for ( uint8_t i = 0; i < this->count; i++ ) {
		fprintf(
			f, "%s%u --> %u%s",
			i == 0 ? "" : ", ",
			this->original[ i ], this->redirected[ i ],
			i == this->count - 1 ? "\n" : ""
		);
	}
}

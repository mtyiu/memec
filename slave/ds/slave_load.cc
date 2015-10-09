#include "slave_load.hh"

SlaveLoad::SlaveLoad() {}

void SlaveLoad::reset() {
	this->_sentBytes = 0;
	this->_recvBytes = 0;

	this->_sealChunk = 0;
	this->_getChunk = 0;
	this->_setChunk = 0;

	Load::reset();
}

void SlaveLoad::aggregate( SlaveLoad &l ) {
	this->_sentBytes += l._sentBytes;
	this->_recvBytes += l._recvBytes;

	this->_sealChunk += l._sealChunk;
	this->_getChunk += l._getChunk;
	this->_setChunk += l._setChunk;
	Load::aggregate( l );
}

void SlaveLoad::print( FILE *f ) {
	int width = 27;
	fprintf( f,
		"%-*s : %lu bytes\n"
		"%-*s : %lu bytes\n",
		width, "Number of bytes sent", this->_sentBytes,
		width, "Number of bytes received", this->_recvBytes
	);
	Load::print( f );
	fprintf( f,
		"%-*s : %u\n"
		"%-*s : %u\n"
		"%-*s : %u\n",
		width, "Number of SEAL_CHUNK operations", this->_sealChunk,
		width, "Number of GET_CHUNK operations", this->_getChunk,
		width, "Number of SET_CHUNK operations", this->_setChunk
	);
}

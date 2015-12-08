#include "protocol.hh"

bool SlaveProtocol::init( size_t size, uint32_t dataChunkCount ) {
	this->status = new bool[ dataChunkCount ];
	return Protocol::init( size );
}

void SlaveProtocol::free() {
	delete[] this->status;
	Protocol::free();
}

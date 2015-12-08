#include "protocol.hh"

bool MasterProtocol::init( size_t size, uint32_t parityChunkCount ) {
	this->status = new bool[ parityChunkCount ];
	return Protocol::init( size );
}

void MasterProtocol::free() {
	delete[] this->status;
	Protocol::free();
}

#include "degraded_chunk_buffer.hh"

DegradedChunkBuffer::DegradedChunkBuffer() {}

void DegradedChunkBuffer::print( FILE *f ) {
	this->map.dump( f );
}

void DegradedChunkBuffer::stop() {}

DegradedChunkBuffer::~DegradedChunkBuffer() {}

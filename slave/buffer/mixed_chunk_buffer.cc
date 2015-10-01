#include "mixed_chunk_buffer.hh"

MixedChunkBuffer::MixedChunkBuffer( DataChunkBuffer *dataChunkBuffer ) {
	this->role = CBR_DATA;
	this->buffer.data = dataChunkBuffer;
}

MixedChunkBuffer::MixedChunkBuffer( ParityChunkBuffer *parityChunkBuffer ) {
	this->role = CBR_PARITY;
	this->buffer.parity = parityChunkBuffer;
}

void MixedChunkBuffer::set( char *key, uint8_t keySize, char *value, uint32_t valueSize, uint8_t opcode, uint32_t chunkId, Chunk **dataChunks, Chunk *dataChunk, Chunk *parityChunk ) {
	switch( this->role ) {
		case CBR_DATA:
			this->buffer.data->set( key, keySize, value, valueSize, opcode );
			break;
		case CBR_PARITY:
		default:
			this->buffer.parity->set( key, keySize, value, valueSize, chunkId, dataChunks, dataChunk, parityChunk );
			break;
	}
}

int MixedChunkBuffer::lockChunk( Chunk *chunk ) {
	switch( this->role ) {
		case CBR_DATA:
			return this->buffer.data->lockChunk( chunk );
		case CBR_PARITY:
		default:
			return -1;
	}
}

void MixedChunkBuffer::updateAndUnlockChunk( int index ) {
	switch( this->role ) {
		case CBR_DATA:
			this->buffer.data->updateAndUnlockChunk( index );
			break;
		case CBR_PARITY:
		default:
			break;
	}
}

void MixedChunkBuffer::update( uint32_t stripeId, uint32_t chunkId, uint32_t offset, uint32_t size, char *dataDelta, Chunk **dataChunks, Chunk *dataChunk, Chunk *parityChunk ) {
	switch( this->role ) {
		case CBR_PARITY:
			this->buffer.parity->update( stripeId, chunkId, offset, size, dataDelta, dataChunks, dataChunk, parityChunk );
			break;
		default:
			return;
	}
}

void MixedChunkBuffer::stop() {
	switch( this->role ) {
		case CBR_DATA:
			this->buffer.data->stop();
			break;
		case CBR_PARITY:
			this->buffer.parity->stop();
			break;
	}
}

void MixedChunkBuffer::print( FILE *f ) {
	switch( this->role ) {
		case CBR_DATA:
			this->buffer.data->print( f );
			break;
		case CBR_PARITY:
			this->buffer.parity->print( f );
			break;
	}
}

MixedChunkBuffer::~MixedChunkBuffer() {
	switch( this->role ) {
		case CBR_DATA:
			delete this->buffer.data;
			break;
		case CBR_PARITY:
			delete this->buffer.parity;
			break;
	}
}

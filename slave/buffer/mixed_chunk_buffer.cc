#include "mixed_chunk_buffer.hh"

MixedChunkBuffer::MixedChunkBuffer( DataChunkBuffer *dataChunkBuffer ) {
	this->role = CBR_DATA;
	this->buffer.data = dataChunkBuffer;
}

MixedChunkBuffer::MixedChunkBuffer( ParityChunkBuffer *parityChunkBuffer ) {
	this->role = CBR_PARITY;
	this->buffer.parity = parityChunkBuffer;
}

KeyMetadata MixedChunkBuffer::set( char *key, uint8_t keySize, char *value, uint32_t valueSize, bool &isParity, uint32_t dataChunkId ) {
	switch( this->role ) {
		case CBR_DATA:
			isParity = false;
			return this->buffer.data->set( key, keySize, value, valueSize );
		case CBR_PARITY:
		default:
			isParity = true;
			return this->buffer.parity->set( key, keySize, value, valueSize, dataChunkId );
	}
}

uint32_t MixedChunkBuffer::flush( bool lock ) {
	switch( this->role ) {
		case CBR_DATA:
			return this->buffer.data->flush( lock );
		case CBR_PARITY:
		default:
			return this->buffer.parity->flush( lock );
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

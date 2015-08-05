#include <cstring>
#include "chunk.hh"
#include "key_value.hh"

uint32_t Chunk::capacity;

Chunk::Chunk() {
	this->status = CHUNK_STATUS_EMPTY;
	this->count = 0;
	this->size = 0;
	this->isParity = false;
	this->data = 0;
}

void Chunk::init( uint32_t capacity ) {
	Chunk::capacity = capacity;
}

void Chunk::init() {
	this->data = new char[ Chunk::capacity ];
	this->clear();
}

char *Chunk::alloc( uint32_t size, uint32_t &offset ) {
	char *ret = this->data + this->size;
	offset = this->size;

	this->status = CHUNK_STATUS_DIRTY;
	this->count++;
	this->size += size;
	
	return ret;
}

void Chunk::updateData() {
	uint8_t keySize;
	uint32_t valueSize, tmp;
	char *key, *value, *ptr = this->data;

	this->isParity = false;
	while ( ptr < this->data + Chunk::capacity ) {
		KeyValue::deserialize( ptr, key, keySize, value, valueSize );

		tmp = KEY_VALUE_METADATA_SIZE + keySize + valueSize;
		
		this->count++;
		this->size += tmp;

		ptr += tmp;
	}
}

void Chunk::updateParity( uint32_t offset, uint32_t length ) {
	uint32_t size = offset + length;
	this->isParity = true;
	if ( size > this->size )
		this->size = size;
}

void Chunk::clear() {
	this->status = CHUNK_STATUS_EMPTY;
	this->count = 0;
	this->size = 0;
	memset( this->data, 0, Chunk::capacity );
}

void Chunk::free() {
	delete this->data;
}

bool Chunk::initFn( Chunk *chunk, void *argv ) {
	chunk->init();
	return true;
}

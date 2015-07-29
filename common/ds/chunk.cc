#include <cstring>
#include "chunk.hh"
#include "key_value.hh"

uint32_t Chunk::capacity;

Chunk::Chunk() {
	this->data = 0;
	this->count = 0;
	this->size = 0;
}

void Chunk::init( uint32_t capacity ) {
	Chunk::capacity = capacity;
}

void Chunk::init() {
	this->data = new char[ Chunk::capacity ];
	this->clear();
}

char *Chunk::alloc( uint32_t size ) {
	char *ret = this->data + this->size;
	this->count++;
	this->size += size;
	return ret;
}

void Chunk::update( bool isParity ) {
	uint8_t keySize;
	uint32_t valueSize, tmp;
	char *key, *value, *ptr = this->data;

	while ( ptr < this->data + Chunk::capacity ) {
		KeyValue::deserialize( ptr, key, keySize, value, valueSize );

		tmp = KEY_VALUE_METADATA_SIZE + keySize + valueSize;
		
		this->count++;
		this->size += tmp;

		ptr += tmp;
	}
}

void Chunk::clear() {
	this->count = 0;
	this->size = 0;
	memset( this->data, 0, Chunk::capacity );
}

void Chunk::free() {
	delete this->data;
}

/*
char *Chunk::serialize() {
	uint32_t tmp;

	tmp = htonl( this->count );
	*( ( uint32_t * ) this->data ) = tmp;

	tmp = htonl( this->size );
	*( ( uint32_t * )( this->data + KEY_VALUE_METADATA_SIZE ) ) = size;

	return this->data;
}

char *Chunk::deserialize() {
	this->count = *( ( uint32_t * ) data );
	this->size = *( ( uint32_t * )( data + KEY_VALUE_METADATA_SIZE ) );
	this->count = ntohl( this->count );
	this->size = ntohl( this->size );
	return this->data + 8;
}
*/

bool Chunk::initFn( Chunk *chunk, void *argv ) {
	chunk->init();
	return true;
}

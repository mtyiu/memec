#include <cstring>
#include <cassert>
#include "chunk.hh"
#include "key_value.hh"
#include "../coding/coding.hh"
#include "../util/debug.hh"

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

void Chunk::computeDelta( char *delta, char *newData, uint32_t offset, uint32_t length, bool update ) {
	Coding::bitwiseXOR(
		delta,
		this->data + offset, // original data
		newData,             // new data
		length
	);
	if ( update ) {
		this->status = CHUNK_STATUS_DIRTY;
		Coding::bitwiseXOR(
			this->data + offset,
			this->data + offset, // original data
			delta,               // new data
			length
		);	
	}
}

uint32_t Chunk::deleteKeyValue( Key target, std::map<Key, KeyMetadata> *keys, char *delta, size_t deltaBufSize ) {
	uint32_t deltaSize, bytes;
	char *startPtr, *src, *dst;
	std::map<Key, KeyMetadata>::iterator it = keys->find( target );
	assert( it != keys->end() );

	// Backup the metadata and then remove the entry from map
	target = it->first;
	KeyMetadata &metadata = it->second;
	keys->erase( it );
	target.free();

	startPtr = this->data + metadata.offset;

	// Update all data after the key
	deltaSize = this->size - metadata.offset;
	if ( deltaSize > deltaBufSize ) {
		__ERROR__( "Chunk", "deleteKeyValue", "The buffer size is smaller than the modified chunk size." );
		return 0;
	}
	// Backup the original data
	memcpy( delta, startPtr, deltaSize );

	// Move all key-value pairs to eliminate the hole
	bytes = metadata.offset + metadata.length;
	dst = startPtr;
	src = startPtr + metadata.length;
	while( bytes < this->size ) {
		uint8_t keySize;
		uint32_t valueSize;
		char *key, *value;
		// Retrieve metadata for the current key-value pair
		KeyValue::deserialize( src, key, keySize, value, valueSize );
		Key tmp;
		tmp.size = keySize;
		tmp.data = key;
		tmp.ptr = 0;

		it = keys->find( tmp );
		assert( it != keys->end() );
		KeyMetadata &m = it->second;

		// Update the offset and relocate the memory
		m.offset = dst - this->data;
		memmove( dst, src, m.length );

		bytes += m.length;
		dst += m.length;
		src += m.length;
	}

	// Update internal counters
	this->count--;
	this->size -= metadata.length;

	this->status = CHUNK_STATUS_DIRTY;

	// Compute data delta
	Coding::bitwiseXOR( delta, delta, startPtr, deltaSize );
	return deltaSize;
}

KeyValue Chunk::getKeyValue( uint32_t offset ) {
	KeyValue ret;
	ret.data = this->data + offset;
	return ret;	
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

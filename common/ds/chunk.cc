#include <cstring>
#include <cassert>
#include "chunk.hh"
#include "key_value.hh"
#include "../coding/coding.hh"
#include "../protocol/protocol.hh"
#include "../util/debug.hh"

uint32_t Chunk::capacity;

Chunk::Chunk() {
	this->data = 0;
	this->size = 0;
	this->status = CHUNK_STATUS_EMPTY;
	this->count = 0;
	this->lastDelPos = 0;
	this->isParity = false;
#ifdef USE_CHUNK_MUTEX_LOCK
	pthread_mutex_init( &this->lock, 0 );
#endif
}

void Chunk::init( uint32_t capacity ) {
	Chunk::capacity = capacity;
}

void Chunk::init() {
	this->data = new char[ Chunk::capacity ];
	this->clear();
}

void Chunk::loadFromGetChunkRequest( uint32_t listId, uint32_t stripeId, uint32_t chunkId, bool isParity, char *data, uint32_t size ) {
#ifdef USE_CHUNK_MUTEX_LOCK
	pthread_mutex_lock( &this->lock );
#endif
	this->status = CHUNK_STATUS_FROM_GET_CHUNK;
	this->size = size;
	this->metadata.set( listId, stripeId, chunkId );
	this->isParity = isParity;
	memcpy( this->data, data, size );
#ifdef USE_CHUNK_MUTEX_LOCK
	pthread_mutex_unlock( &this->lock );
#endif
}

void Chunk::swap( Chunk *c ) {
	Chunk tmp;

	tmp.status = c->status;
	tmp.count = c->count;
	tmp.size = c->size;
	tmp.metadata = c->metadata;
	tmp.isParity = c->isParity;
	tmp.data = c->data;

#ifdef USE_CHUNK_MUTEX_LOCK
	pthread_mutex_lock( &c->lock );
#endif
	c->status = this->status;
	c->count = this->count;
	c->size = this->size;
	c->metadata = this->metadata;
	c->isParity = this->isParity;
	c->data = this->data;
#ifdef USE_CHUNK_MUTEX_LOCK
	pthread_mutex_unlock( &c->lock );
#endif

#ifdef USE_CHUNK_MUTEX_LOCK
	pthread_mutex_lock( &this->lock );
#endif
	this->status = tmp.status;
	this->count = tmp.count;
	this->size = tmp.size;
	this->metadata = tmp.metadata;
	this->isParity = tmp.isParity;
	this->data = tmp.data;
#ifdef USE_CHUNK_MUTEX_LOCK
	pthread_mutex_unlock( &this->lock );
#endif
}

char *Chunk::alloc( uint32_t size, uint32_t &offset ) {
#ifdef USE_CHUNK_MUTEX_LOCK
	pthread_mutex_lock( &this->lock );
#endif
	char *ret = this->data + this->size;
	offset = this->size;

	this->status = CHUNK_STATUS_DIRTY;
	this->count++;
	this->size += size;
#ifdef USE_CHUNK_MUTEX_LOCK
	pthread_mutex_unlock( &this->lock );
#endif

	return ret;
}

#ifdef USE_CHUNK_MUTEX_LOCK
int Chunk::next( int offset, char *&key, uint8_t &keySize, bool needsLock, bool needsUnlock )
#else
int Chunk::next( int offset, char *&key, uint8_t &keySize )
#endif
{
#ifdef USE_CHUNK_MUTEX_LOCK
	if ( needsLock ) pthread_mutex_lock( &this->lock );
#endif
	char *ptr = this->data + offset, *value;
	int ret = -1;
	uint32_t valueSize;

	if ( ptr < this->data + Chunk::capacity ) {
		KeyValue::deserialize( ptr, key, keySize, value, valueSize );
		if ( keySize )
			ret = offset + PROTO_KEY_VALUE_SIZE + keySize + valueSize;
		else
			key = 0;
	}
#ifdef USE_CHUNK_MUTEX_LOCK
	if ( needsUnlock ) pthread_mutex_unlock( &this->lock );
#endif
	return ret;
}

uint32_t Chunk::updateData() {
	uint8_t keySize;
	uint32_t valueSize, tmp;
	char *key, *value, *ptr = this->data;

#ifdef USE_CHUNK_MUTEX_LOCK
	pthread_mutex_lock( &this->lock );
#endif
	this->isParity = false;
	while ( ptr < this->data + Chunk::capacity ) {
		KeyValue::deserialize( ptr, key, keySize, value, valueSize );
		if ( keySize == 0 && valueSize == 0 )
			break;

		tmp = KEY_VALUE_METADATA_SIZE + keySize + valueSize;

		this->count++;
		this->size += tmp;

		ptr += tmp;
	}
	tmp = this->size;
#ifdef USE_CHUNK_MUTEX_LOCK
	pthread_mutex_unlock( &this->lock );
#endif
	return tmp;
}

uint32_t Chunk::updateParity( uint32_t offset, uint32_t length ) {
#ifdef USE_CHUNK_MUTEX_LOCK
	pthread_mutex_lock( &this->lock );
#endif
	uint32_t size = offset + length;
	this->isParity = true;
	if ( size > this->size )
		this->size = size;
	size = this->size;
#ifdef USE_CHUNK_MUTEX_LOCK
	pthread_mutex_unlock( &this->lock );
#endif
	return size;
}

void Chunk::computeDelta( char *delta, char *newData, uint32_t offset, uint32_t length, bool update ) {
#ifdef USE_CHUNK_MUTEX_LOCK
	pthread_mutex_lock( &this->lock );
#endif
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
#ifdef USE_CHUNK_MUTEX_LOCK
	pthread_mutex_unlock( &this->lock );
#endif
}

void Chunk::update( char *newData, uint32_t offset, uint32_t length ) {
#ifdef USE_CHUNK_MUTEX_LOCK
	pthread_mutex_lock( &this->lock );
#endif
	memcpy( this->data + offset, newData, length );
#ifdef USE_CHUNK_MUTEX_LOCK
	pthread_mutex_unlock( &this->lock );
#endif
}

uint32_t Chunk::deleteKeyValue( Key target, std::unordered_map<Key, KeyMetadata> *keys, char *delta, size_t deltaBufSize ) {
#ifdef USE_CHUNK_MUTEX_LOCK
	pthread_mutex_lock( &this->lock );
#endif

	uint32_t deltaSize, bytes;
	char *startPtr, *src, *dst;
	std::unordered_map<Key, KeyMetadata>::iterator it = keys->find( target );
	assert( it != keys->end() );

	// Backup the metadata and then remove the entry from map
	target = it->first;
	KeyMetadata metadata = it->second;
	keys->erase( it );
	target.free();

	startPtr = this->data + metadata.offset;

	// Update all data after the key
	deltaSize = this->size - metadata.offset;
	if ( delta ) {
		if ( deltaSize > deltaBufSize ) {
			__ERROR__( "Chunk", "deleteKeyValue", "The buffer size (%lu) is smaller than the modified chunk size (%u).", deltaBufSize, deltaSize );
#ifdef USE_CHUNK_MUTEX_LOCK
			pthread_mutex_unlock( &this->lock );
#endif
			return 0;
		}

		// Backup the original data
		memcpy( delta, startPtr, deltaSize );
	}

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
		tmp.set( keySize, key, 0 );

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
	// Set the data after the last key-value pair as zeros
	memset( dst, 0, metadata.length );

	// Update internal counters
	this->count--;
	this->size -= metadata.length;
	this->status = CHUNK_STATUS_DIRTY;

	if ( delta ) {
		// Update counter if the chunk is sealed before
		this->lastDelPos = this->size;
		// Compute data delta
		Coding::bitwiseXOR( delta, delta, startPtr, deltaSize );
	}

#ifdef USE_CHUNK_MUTEX_LOCK
	pthread_mutex_unlock( &this->lock );
#endif
	return deltaSize;
}

KeyValue Chunk::getKeyValue( uint32_t offset ) {
	KeyValue ret;
#ifdef USE_CHUNK_MUTEX_LOCK
	pthread_mutex_lock( &this->lock );
#endif
	ret.data = this->data + offset;
#ifdef USE_CHUNK_MUTEX_LOCK
	pthread_mutex_unlock( &this->lock );
#endif
	return ret;
}

void Chunk::clear() {
#ifdef USE_CHUNK_MUTEX_LOCK
	pthread_mutex_lock( &this->lock );
#endif
	this->status = CHUNK_STATUS_EMPTY;
	this->count = 0;
	this->size = 0;
	memset( this->data, 0, Chunk::capacity );
#ifdef USE_CHUNK_MUTEX_LOCK
	pthread_mutex_unlock( &this->lock );
#endif
}

void Chunk::setReconstructed( uint32_t listId, uint32_t stripeId, uint32_t chunkId, bool isParity ) {
#ifdef USE_CHUNK_MUTEX_LOCK
	pthread_mutex_lock( &this->lock );
#endif
	this->status = CHUNK_STATUS_RECONSTRUCTED;
	this->count = 0;
	this->size = 0;
	this->metadata.listId = listId;
	this->metadata.stripeId = stripeId;
	this->metadata.chunkId = chunkId;
	this->isParity = isParity;
	memset( this->data, 0, Chunk::capacity );
#ifdef USE_CHUNK_MUTEX_LOCK
	pthread_mutex_unlock( &this->lock );
#endif
}

void Chunk::free() {
#ifdef USE_CHUNK_MUTEX_LOCK
	pthread_mutex_lock( &this->lock );
#endif
	delete this->data;
	this->data = 0;
#ifdef USE_CHUNK_MUTEX_LOCK
	pthread_mutex_unlock( &this->lock );
#endif
}

bool Chunk::initFn( Chunk *chunk, void *argv ) {
	chunk->init();
	return true;
}

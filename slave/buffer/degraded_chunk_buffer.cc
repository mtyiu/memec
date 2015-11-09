#include "degraded_chunk_buffer.hh"

DegradedChunkBuffer::DegradedChunkBuffer() {}

void DegradedChunkBuffer::init( Map *slaveMap ) {
	this->slaveMap = slaveMap;
}

void DegradedChunkBuffer::print( FILE *f ) {
	this->map.dump( f );
}

void DegradedChunkBuffer::stop() {}

bool DegradedChunkBuffer::updateKeyValue( uint8_t keySize, char *keyStr, uint32_t valueUpdateSize, uint32_t valueUpdateOffset, uint32_t chunkUpdateOffset, char *valueUpdate, Chunk *chunk, bool isSealed ) {
	Key key;
	LOCK_T *keysLock, *cacheLock;
	std::unordered_map<Key, KeyMetadata> *keys;
	std::unordered_map<Metadata, Chunk *> *cache;

	this->map.getKeysMap( keys, keysLock );
	this->map.getCacheMap( cache, cacheLock );

	key.set( keySize, keyStr );

	LOCK( keysLock );
	if ( isSealed ) {
		LOCK( cacheLock );
		chunk->computeDelta(
			valueUpdate, // delta
			valueUpdate, // new data
			chunkUpdateOffset,
			valueUpdateSize,
			true // perform update
		);
		UNLOCK( cacheLock );
	} else {

	}
	UNLOCK( keysLock );

	return false;
}

bool DegradedChunkBuffer::deleteKey( uint8_t opcode, uint8_t keySize, char *keyStr, bool isSealed, uint32_t &deltaSize, char *delta, Chunk *chunk ) {
	Key key;
	KeyMetadata keyMetadata;
	bool ret;
	LOCK_T *keysLock, *cacheLock;
	std::unordered_map<Key, KeyMetadata> *keys;
	std::unordered_map<Metadata, Chunk *> *cache;

	this->map.getKeysMap( keys, keysLock );
	this->map.getCacheMap( cache, cacheLock );

	key.set( keySize, keyStr );

	LOCK( keysLock );
	if ( isSealed ) {
		LOCK( cacheLock );
		ret = this->map.deleteKey( key, opcode, keyMetadata, false, false );
		if ( ret ) {
			deltaSize = chunk->deleteKeyValue(
				keys, keyMetadata, delta, deltaSize );
			ret = true;
		}
		UNLOCK( cacheLock );
	} else {
		ret = this->map.deleteValue( key, opcode );
	}
	UNLOCK( keysLock );

	if ( ret )
		ret = this->slaveMap->insertOpMetadata( opcode, key, keyMetadata );

	return ret;
}

DegradedChunkBuffer::~DegradedChunkBuffer() {}

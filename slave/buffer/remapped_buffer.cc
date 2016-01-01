#include "remapped_buffer.hh"

RemappedBuffer::RemappedBuffer() {
	LOCK_INIT( &this->keysLock );
}

bool RemappedBuffer::insert( uint32_t listId, uint32_t chunkId, uint32_t *original, uint32_t *remapped, uint32_t remappedCount, char *keyStr, uint8_t keySize, char *valueStr, uint32_t valueSize ) {
	RemappedKeyValue remappedKeyValue;

	remappedKeyValue.listId = listId;
	remappedKeyValue.chunkId = chunkId;
	remappedKeyValue.remappedCount = remappedCount;
	remappedKeyValue.key = new char[ keySize ];
	remappedKeyValue.keySize = keySize;
	remappedKeyValue.value = new char[ valueSize ];
	remappedKeyValue.valueSize = valueSize;

	memcpy( remappedKeyValue.key, keyStr, keySize );
	memcpy( remappedKeyValue.value, valueStr, valueSize );

	if ( remappedCount ) {
		remappedKeyValue.original = new uint32_t[ remappedCount * 2 ];
		remappedKeyValue.remapped = new uint32_t[ remappedCount * 2 ];
		for ( uint32_t i = 0; i < remappedCount; i++ ) {
			remappedKeyValue.original[ i * 2     ] = original[ i * 2     ];
			remappedKeyValue.original[ i * 2 + 1 ] = original[ i * 2 + 1 ];
			remappedKeyValue.remapped[ i * 2     ] = remapped[ i * 2     ];
			remappedKeyValue.remapped[ i * 2 + 1 ] = remapped[ i * 2 + 1 ];
		}
	} else {
		remappedKeyValue.original = 0;
		remappedKeyValue.remapped = 0;
	}

	Key key;
	key.set( keySize, remappedKeyValue.key );

	std::pair<Key, RemappedKeyValue> p( key, remappedKeyValue );
	std::pair<std::unordered_map<Key, RemappedKeyValue>::iterator, bool> ret;

	LOCK( &this->keysLock );
	ret = this->keys.insert( p );
	UNLOCK( &this->keysLock );

	if ( ! ret.second ) {
		if ( remappedCount ) {
			delete[] remappedKeyValue.original;
			delete[] remappedKeyValue.remapped;
		}
		delete[] remappedKeyValue.key;
		delete[] remappedKeyValue.value;
	}

	return ret.second;
}

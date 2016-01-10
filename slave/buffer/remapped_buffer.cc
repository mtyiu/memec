#include "remapped_buffer.hh"

RemappedBuffer::RemappedBuffer() {
	LOCK_INIT( &this->keysLock );
}

bool RemappedBuffer::insert( uint32_t listId, uint32_t chunkId, uint32_t *original, uint32_t *remapped, uint32_t remappedCount, char *keyStr, uint8_t keySize, char *valueStr, uint32_t valueSize ) {
	RemappedKeyValue remappedKeyValue;

	remappedKeyValue.listId = listId;
	remappedKeyValue.chunkId = chunkId;
	remappedKeyValue.remappedCount = remappedCount;
	remappedKeyValue.keyValue.dup( keyStr, keySize, valueStr, valueSize );

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

	Key key = remappedKeyValue.keyValue.key();

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
		remappedKeyValue.keyValue.free();
	}

	return ret.second;
}

bool RemappedBuffer::update( uint8_t keySize, char *keyStr, uint32_t valueUpdateSize, uint32_t valueUpdateOffset, char *valueUpdate, RemappedKeyValue *remappedKeyValue ) {
	std::unordered_map<Key, RemappedKeyValue>::iterator it;
	Key key;
	bool ret;
	key.set( keySize, keyStr );

	LOCK( &this->keysLock );
	it = this->keys.find( key );
	ret = ( it != this->keys.end() );
	if ( ret ) {
		// Perform update
		uint32_t offset = KEY_VALUE_METADATA_SIZE + keySize + valueUpdateOffset;
		KeyValue &keyValue = it->second.keyValue;
		memcpy( keyValue.data + offset, valueUpdate, valueUpdateSize );
		if ( remappedKeyValue ) *remappedKeyValue = it->second;
	}
	UNLOCK( &this->keysLock );

	return ret;
}

bool RemappedBuffer::find( uint8_t keySize, char *keyStr, RemappedKeyValue *remappedKeyValue ) {
	std::unordered_map<Key, RemappedKeyValue>::iterator it;
	Key key;
	bool ret;
	key.set( keySize, keyStr );

	LOCK( &this->keysLock );
	it = this->keys.find( key );
	ret = ( it != this->keys.end() );
	if ( ret && remappedKeyValue ) {
		*remappedKeyValue = it->second;
	}
	UNLOCK( &this->keysLock );

	return ret;
}

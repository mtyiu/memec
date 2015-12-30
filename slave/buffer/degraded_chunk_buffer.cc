#include "degraded_chunk_buffer.hh"

DegradedMap::DegradedMap() {
	LOCK_INIT( &this->keysLock );
	LOCK_INIT( &this->unsealed.lock );
	LOCK_INIT( &this->cacheLock );
	LOCK_INIT( &this->degraded.chunksLock );
	LOCK_INIT( &this->degraded.keysLock );
}

void DegradedMap::init( Map *map ) {
	this->slaveMap = map;
}

bool DegradedMap::findValueByKey( char *data, uint8_t size, bool &isSealed, KeyValue *keyValue, Key *keyPtr, KeyMetadata *keyMetadataPtr, Metadata *metadataPtr, Chunk **chunkPtr ) {
	std::unordered_map<Key, KeyMetadata>::iterator keysIt;
	std::unordered_map<Metadata, Chunk *>::iterator cacheIt;
	Key key;
	Chunk *chunk;

	isSealed = false;

	if ( keyValue )
		keyValue->clear();
	key.set( size, data );

	LOCK( &this->keysLock );
	keysIt = this->keys.find( key );
	if ( keysIt == this->keys.end() ) {
		if ( keyPtr ) *keyPtr = key;
		UNLOCK( &this->keysLock );
		goto find_in_values;
	} else {
		isSealed = true;
	}

	if ( keyPtr ) *keyPtr = keysIt->first;
	if ( keyMetadataPtr ) *keyMetadataPtr = keysIt->second;
	UNLOCK( &this->keysLock );

	LOCK( &this->cacheLock );
	cacheIt = this->cache.find( keysIt->second );
	if ( cacheIt == this->cache.end() ) {
		UNLOCK( &this->cacheLock );
		return false;
	}

	if ( metadataPtr ) *metadataPtr = cacheIt->first;
	if ( chunkPtr ) *chunkPtr = cacheIt->second;

	chunk = cacheIt->second;
	if ( keyValue )
		*keyValue = chunk->getKeyValue( keysIt->second.offset );
	UNLOCK( &this->cacheLock );
	return true;

find_in_values:
	isSealed = false;

	std::unordered_map<Key, KeyValue>::iterator it;

	LOCK( &this->unsealed.lock );
	it = this->unsealed.values.find( key );
	if ( it == this->unsealed.values.end() ) {
		UNLOCK( &this->unsealed.lock );
		return false;
	} else {
		if ( keyPtr ) *keyPtr = it->first;
		*keyValue = it->second;
	}
	UNLOCK( &this->unsealed.lock );

	return true;
}

Chunk *DegradedMap::findChunkById( uint32_t listId, uint32_t stripeId, uint32_t chunkId, Metadata *metadataPtr ) {
	std::unordered_map<Metadata, Chunk *>::iterator it;
	Metadata metadata;

	metadata.set( listId, stripeId, chunkId );
	if ( metadataPtr ) *metadataPtr = metadata;

	LOCK( &this->cacheLock );
	it = this->cache.find( metadata );
	if ( it == this->cache.end() ) {
		UNLOCK( &this->cacheLock );
		return 0;
	}
	UNLOCK( &this->cacheLock );
	return it->second;
}

bool DegradedMap::insertKey( Key key, uint8_t opcode, KeyMetadata &keyMetadata ) {
	OpMetadata opMetadata;
	opMetadata.clone( keyMetadata );
	opMetadata.opcode = opcode;

	Key k1;

	k1.dup( key.size, key.data );
	std::pair<Key, KeyMetadata> keyPair( k1, keyMetadata );
	std::pair<std::unordered_map<Key, KeyMetadata>::iterator, bool> keyRet;

	LOCK( &this->keysLock );
	keyRet = this->keys.insert( keyPair );
	if ( ! keyRet.second ) {
		UNLOCK( &this->keysLock );
		return false;
	}
	UNLOCK( &this->keysLock );

	return true;
}

bool DegradedMap::deleteKey( Key key, uint8_t opcode, uint32_t &timestamp, KeyMetadata &keyMetadata, bool needsLock, bool needsUnlock ) {
	Key k;
	std::unordered_map<Key, KeyMetadata>::iterator keysIt;

	if ( needsLock ) LOCK( &this->keysLock );
	keysIt = this->keys.find( key );
	if ( keysIt == this->keys.end() ) {
		if ( needsUnlock ) UNLOCK( &this->keysLock );
		printf( "deleteKey(): Key not found.\n" );
		return false;
	} else {
		k = keysIt->first;
		keyMetadata = keysIt->second;
		this->keys.erase( keysIt );
		k.free();
	}
	if ( needsUnlock ) UNLOCK( &this->keysLock );

	return this->slaveMap->insertOpMetadata( opcode, timestamp, key, keyMetadata );
}

bool DegradedMap::insertValue( KeyValue keyValue, Metadata metadata ) { // KeyValue's data is allocated by malloc()
	Key key = keyValue.key();
	std::pair<Key, KeyValue> p1( key, keyValue );
	std::pair<Metadata, Key> p2( metadata, key );
	std::pair<std::unordered_map<Key, KeyValue>::iterator, bool> ret;

	LOCK( &this->unsealed.lock );
	ret = this->unsealed.values.insert( p1 );
	if ( ret.second )
		this->unsealed.metadataRev.insert( p2 );
	UNLOCK( &this->unsealed.lock );

	return ret.second;
}

bool DegradedMap::deleteValue( Key key, Metadata metadata, uint8_t opcode, uint32_t &timestamp ) {
	std::unordered_map<Key, KeyValue>::iterator valuesIt;
	std::unordered_map<Key, Metadata>::iterator metadataIt;
	std::pair<
		std::unordered_multimap<Metadata, Key>::iterator,
		std::unordered_multimap<Metadata, Key>::iterator
	> metadataRevIts;
	std::unordered_multimap<Metadata, Key>::iterator metadataRevIt;
	KeyValue keyValue;
	bool isFound = false;

	LOCK( &this->unsealed.lock );
	valuesIt = this->unsealed.values.find( key );
	if ( valuesIt == this->unsealed.values.end() )
		goto re_insert;
	keyValue = valuesIt->second;
	this->unsealed.values.erase( valuesIt );

	// Find from metadataRev
	metadataRevIts = this->unsealed.metadataRev.equal_range( metadata );
	for ( metadataRevIt = metadataRevIts.first; metadataRevIt != metadataRevIts.second; metadataRevIt++ ) {
		if ( metadataRevIt->second == key ) {
			this->unsealed.metadataRev.erase( metadataRevIt );
			isFound = true;
			break;
		}
	}
	assert( isFound );
	keyValue.free();

re_insert:
	// Re-insert for synchronizing the unsealed chunks
	key.dup();
	std::pair<Metadata, Key> p( metadata, key );
	this->unsealed.metadataRev.insert( p );
	this->unsealed.deleted.insert( key );

	UNLOCK( &this->unsealed.lock );

	KeyMetadata keyMetadata;
	keyMetadata.set( metadata.listId, metadata.stripeId, metadata.chunkId );

	return this->slaveMap->insertOpMetadata( opcode, timestamp, key, keyMetadata );
}

bool DegradedMap::insertDegradedChunk( uint32_t listId, uint32_t stripeId, uint32_t chunkId, uint16_t instanceId, uint32_t requestId ) {
	Metadata metadata;
	std::unordered_map<Metadata, std::vector<struct pid_s>>::iterator it;
	bool ret = false;

	metadata.set( listId, stripeId, chunkId );

	LOCK( &this->degraded.chunksLock );
	it = this->degraded.chunks.find( metadata );
	if ( it == this->degraded.chunks.end() ) {
		std::vector<struct pid_s> pids;
		struct pid_s data = { .instanceId = instanceId, .requestId = requestId };
		pids.push_back( data );

		std::pair<Metadata, std::vector<struct pid_s>> p( metadata, pids );
		std::pair<std::unordered_map<Metadata, std::vector<struct pid_s>>::iterator, bool> r;

		r = this->degraded.chunks.insert( p );

		ret = r.second;
	} else {
		std::vector<struct pid_s> &pids = it->second;
		struct pid_s data = { .instanceId = instanceId, .requestId = requestId };
		pids.push_back( data );
		ret = false;
	}
	UNLOCK( &this->degraded.chunksLock );

	return ret;
}

bool DegradedMap::deleteDegradedChunk( uint32_t listId, uint32_t stripeId, uint32_t chunkId, std::vector<struct pid_s> &pids ) {
	Metadata metadata;
	std::unordered_map<Metadata, std::vector<struct pid_s>>::iterator it;

	metadata.set( listId, stripeId, chunkId );

	LOCK( &this->degraded.chunksLock );
	it = this->degraded.chunks.find( metadata );
	if ( it == this->degraded.chunks.end() ) {
		UNLOCK( &this->degraded.chunksLock );
		return false;
	} else {
		pids = it->second;
		this->degraded.chunks.erase( it );
	}
	UNLOCK( &this->degraded.chunksLock );

	return true;
}

bool DegradedMap::insertDegradedKey( Key key, uint16_t instanceId, uint32_t requestId ) {
	std::unordered_map<Key, std::vector<struct pid_s>>::iterator it;
	bool ret = false;

	LOCK( &this->degraded.keysLock );
	it = this->degraded.keys.find( key );
	if ( it == this->degraded.keys.end() ) {
		std::vector<struct pid_s> pids;
		struct pid_s data = { .instanceId = instanceId, .requestId = requestId };
		pids.push_back( data );

		std::pair<Key, std::vector<struct pid_s>> p( key, pids );
		std::pair<std::unordered_map<Key, std::vector<struct pid_s>>::iterator, bool> r;

		r = this->degraded.keys.insert( p );

		ret = r.second;
	} else {
		std::vector<struct pid_s> &pids = it->second;
		struct pid_s data = { .instanceId = instanceId, .requestId = requestId };

		pids.push_back( data );
		ret = false;
	}
	UNLOCK( &this->degraded.keysLock );

	return ret;
}

bool DegradedMap::deleteDegradedKey( Key key, std::vector<struct pid_s> &pids ) {
	std::unordered_map<Key, std::vector<struct pid_s>>::iterator it;

	LOCK( &this->degraded.keysLock );
	it = this->degraded.keys.find( key );
	if ( it == this->degraded.keys.end() ) {
		UNLOCK( &this->degraded.keysLock );
		return false;
	} else {
		pids = it->second;
		this->degraded.keys.erase( it );
	}
	UNLOCK( &this->degraded.keysLock );

	return true;
}

bool DegradedMap::insertChunk( uint32_t listId, uint32_t stripeId, uint32_t chunkId, Chunk *chunk, bool isParity ) {
	Metadata metadata;
	metadata.set( listId, stripeId, chunkId );

	std::pair<Metadata, Chunk *> p( metadata, chunk );
	std::pair<std::unordered_map<Metadata, Chunk *>::iterator, bool> ret;

	LOCK( &this->cacheLock );
	ret = this->cache.insert( p );
	UNLOCK( &this->cacheLock );

	if ( ret.second && ! isParity ) {
		char *ptr = chunk->getData();
		char *keyPtr, *valuePtr;
		uint8_t keySize;
		uint32_t valueSize, offset = 0, size;

		LOCK( &this->keysLock );
		while( ptr < chunk->getData() + Chunk::capacity ) {
			KeyValue::deserialize( ptr, keyPtr, keySize, valuePtr, valueSize );
			if ( keySize == 0 && valueSize == 0 )
				break;

			Key key;
			KeyMetadata keyMetadata;

			size = KEY_VALUE_METADATA_SIZE + keySize + valueSize;

			key.dup( keySize, keyPtr );
			keyMetadata.set( listId, stripeId, chunkId );
			keyMetadata.offset = offset;
			keyMetadata.length = size;

			offset += size;

			this->keys[ key ] = keyMetadata;

			ptr += size;
		}
		UNLOCK( &this->keysLock );
	}

	return ret.second;
}

Chunk *DegradedMap::deleteChunk( uint32_t listId, uint32_t stripeId, uint32_t chunkId, Metadata *metadataPtr ) {
	Metadata metadata;
	Chunk *chunk = 0;
	std::unordered_map<Metadata, Chunk *>::iterator it;

	metadata.set( listId, stripeId, chunkId );
	if ( metadataPtr ) *metadataPtr = metadata;

	LOCK( &this->cacheLock );
	it = this->cache.find( metadata );
	if ( it != this->cache.end() ) {
		// Chunk is found
		chunk = it->second;
		this->cache.erase( it );
	}
	UNLOCK( &this->cacheLock );

	return chunk;
}

void DegradedMap::getKeysMap( std::unordered_map<Key, KeyMetadata> *&keys, LOCK_T *&lock ) {
	keys = &this->keys;
	lock = &this->keysLock;
}

void DegradedMap::getCacheMap( std::unordered_map<Metadata, Chunk *> *&cache, LOCK_T *&lock ) {
	cache = &this->cache;
	lock = &this->cacheLock;
}

void DegradedMap::dump( FILE *f ) {
	fprintf( f, "List of key-value pairs:\n------------------------\n" );
	if ( ! this->keys.size() ) {
		fprintf( f, "(None)\n" );
	} else {
		for ( std::unordered_map<Key, KeyMetadata>::iterator it = this->keys.begin(); it != this->keys.end(); it++ ) {
			fprintf(
				f, "%.*s --> (list ID: %u, stripe ID: %u, chunk ID: %u, offset: %u, length: %u)\n",
				it->first.size, it->first.data,
				it->second.listId, it->second.stripeId, it->second.chunkId,
				it->second.offset, it->second.length
			);
		}
	}
	fprintf( f, "\n" );

	fprintf( f, "Number of key-value pairs: %lu\n\n", this->keys.size() );

	fprintf( f, "List of chunks in the cache:\n----------------------------\n" );
	if ( ! this->cache.size() ) {
		fprintf( f, "(None)\n" );
	} else {
		for ( std::unordered_map<Metadata, Chunk *>::iterator it = this->cache.begin(); it != this->cache.end(); it++ ) {
			fprintf(
				f, "(list ID: %u, stripe ID: %u, chunk ID: %u) --> %p (type: %s chunk, status: %s, count: %u, size: %u)\n",
				it->first.listId, it->first.stripeId, it->first.chunkId,
				it->second, it->second->isParity ? "parity" : "data",
				( it->second->status == CHUNK_STATUS_EMPTY ? "empty" :
					( it->second->status == CHUNK_STATUS_DIRTY ? "dirty" : "cached" )
				),
				it->second->count, it->second->getSize()
			);
		}
	}
	fprintf( f, "\n" );
}

////////////////////////////////////////////////////////////////////////////////

DegradedChunkBuffer::DegradedChunkBuffer() {}

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

bool DegradedChunkBuffer::deleteKey( uint8_t opcode, uint32_t &timestamp, uint8_t keySize, char *keyStr, Metadata metadata, bool isSealed, uint32_t &deltaSize, char *delta, Chunk *chunk ) {
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
		ret = this->map.deleteKey( key, opcode, timestamp, keyMetadata, false, false );
		if ( ret ) {
			deltaSize = chunk->deleteKeyValue( keys, keyMetadata, delta, deltaSize );
			ret = true;
		}
		UNLOCK( cacheLock );
	} else {
		ret = this->map.deleteValue( key, metadata, opcode, timestamp );
	}
	UNLOCK( keysLock );

	return ret;
}

DegradedChunkBuffer::~DegradedChunkBuffer() {}

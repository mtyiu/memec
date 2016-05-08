#include "degraded_chunk_buffer.hh"

DegradedMap::DegradedMap() {
	LOCK_INIT( &this->keysLock );
	LOCK_INIT( &this->unsealed.lock );
	LOCK_INIT( &this->cacheLock );
	LOCK_INIT( &this->removedLock );
	LOCK_INIT( &this->degraded.chunksLock );
	LOCK_INIT( &this->degraded.keysLock );
}

void DegradedMap::init( Map *map ) {
	this->serverMap = map;
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
		*keyValue = ChunkUtil::getObject( chunk, keysIt->second.offset );
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
		if ( keyValue ) *keyValue = it->second;
	}
	UNLOCK( &this->unsealed.lock );

	return true;
}

Chunk *DegradedMap::findChunkById( uint32_t listId, uint32_t stripeId, uint32_t chunkId, Metadata *metadataPtr, bool needsLock, bool needsUnlock ) {
	std::unordered_map<Metadata, Chunk *>::iterator it;
	Metadata metadata;

	metadata.set( listId, stripeId, chunkId );
	if ( metadataPtr ) *metadataPtr = metadata;

	if ( needsLock ) LOCK( &this->cacheLock );
	it = this->cache.find( metadata );
	if ( it == this->cache.end() ) {
		if ( needsUnlock ) UNLOCK( &this->cacheLock );
		return 0;
	}
	if ( needsUnlock ) UNLOCK( &this->cacheLock );
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

	return this->serverMap->insertOpMetadata( opcode, timestamp, key, keyMetadata );
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

	return this->serverMap->insertOpMetadata( opcode, timestamp, key, keyMetadata );
}

bool DegradedMap::insertDegradedChunk( uint32_t listId, uint32_t stripeId, uint32_t chunkId, uint16_t instanceId, uint32_t requestId, bool &isReconstructed ) {
	Metadata metadata;
	std::unordered_map<Metadata, std::vector<struct pid_s>>::iterator it;
	bool ret = false;

	isReconstructed = false;
	metadata.set( listId, stripeId, -1 ); // chunkId );

	LOCK( &this->degraded.chunksLock );
	it = this->degraded.chunks.find( metadata );
	if ( it == this->degraded.chunks.end() ) {
		if ( this->degraded.reconstructedChunks.find( metadata ) == this->degraded.reconstructedChunks.end() ) {
			// Reconstruction in progress
			std::vector<struct pid_s> pids;
			struct pid_s data = { .instanceId = instanceId, .requestId = requestId, .chunkId = chunkId };
			pids.push_back( data );

			std::pair<Metadata, std::vector<struct pid_s>> p( metadata, pids );
			std::pair<std::unordered_map<Metadata, std::vector<struct pid_s>>::iterator, bool> r;

			r = this->degraded.chunks.insert( p );

			ret = r.second;
		} else {
			isReconstructed = true;
			UNLOCK( &this->degraded.chunksLock );
			return false;
		}
	} else {
		std::vector<struct pid_s> &pids = it->second;
		struct pid_s data = { .instanceId = instanceId, .requestId = requestId, .chunkId = chunkId };
		pids.push_back( data );
		ret = false;
	}
	UNLOCK( &this->degraded.chunksLock );

	return ret;
}

bool DegradedMap::insertDegradedChunk( uint32_t listId, uint32_t stripeId, uint32_t chunkId, bool &isReconstructed ) {
	Metadata metadata;
	std::unordered_map<Metadata, std::vector<struct pid_s>>::iterator it;
	bool ret = false;

	isReconstructed = false;
	metadata.set( listId, stripeId, -1 ); // chunkId );

	LOCK( &this->degraded.chunksLock );
	it = this->degraded.chunks.find( metadata );
	if ( it == this->degraded.chunks.end() ) {
		if ( this->degraded.reconstructedChunks.find( metadata ) == this->degraded.reconstructedChunks.end() ) {
			// Reconstruction in progress
			std::vector<struct pid_s> pids;

			// Not applicable here
			// struct pid_s data = { .instanceId = instanceId, .requestId = requestId };
			// pids.push_back( data );

			std::pair<Metadata, std::vector<struct pid_s>> p( metadata, pids );
			std::pair<std::unordered_map<Metadata, std::vector<struct pid_s>>::iterator, bool> r;

			r = this->degraded.chunks.insert( p );

			ret = r.second;
		} else {
			isReconstructed = true;
			UNLOCK( &this->degraded.chunksLock );
			return false;
		}
	} else {
		// std::vector<struct pid_s> &pids = it->second;
		// struct pid_s data = { .instanceId = instanceId, .requestId = requestId };
		// pids.push_back( data );
		ret = false;
	}
	UNLOCK( &this->degraded.chunksLock );

	return ret;
}

bool DegradedMap::deleteDegradedChunk( uint32_t listId, uint32_t stripeId, uint32_t chunkId, std::vector<struct pid_s> &pids, bool force, bool ignoreChunkId ) {
	Metadata metadata;
	std::unordered_map<Metadata, std::vector<struct pid_s>>::iterator it;

	metadata.set( listId, stripeId, -1 ); // chunkId );

	LOCK( &this->degraded.chunksLock );
	it = this->degraded.chunks.find( metadata );
	if ( it == this->degraded.chunks.end() ) {
		if ( force )
			this->degraded.reconstructedChunks.insert( metadata );
		UNLOCK( &this->degraded.chunksLock );
		return false;
	} else {
		std::vector<struct pid_s>::iterator pidsIt;
		for ( pidsIt = it->second.begin(); pidsIt != it->second.end(); ) {
			struct pid_s pid = *pidsIt;
			if ( ignoreChunkId || pid.chunkId == chunkId ) {
				pids.push_back( pid );
				pidsIt = it->second.erase( pidsIt );
			} else {
				pidsIt++;
			}
		}

		if ( it->second.size() == 0 ) {
			this->degraded.chunks.erase( it );
		}
		this->degraded.reconstructedChunks.insert( metadata );
	}
	UNLOCK( &this->degraded.chunksLock );

	return true;
}

bool DegradedMap::insertDegradedKey( Key key, uint16_t instanceId, uint32_t requestId, bool &isReconstructed ) {
	std::unordered_map<Key, std::vector<struct pid_s>>::iterator it;
	bool ret = false;

	isReconstructed = false;

	LOCK( &this->degraded.keysLock );
	it = this->degraded.keys.find( key );
	if ( it == this->degraded.keys.end() ) {
		if ( this->degraded.reconstructedKeys.find( key ) == this->degraded.reconstructedKeys.end() ) {
			// Reconstruction in progress
			std::vector<struct pid_s> pids;
			struct pid_s data = { .instanceId = instanceId, .requestId = requestId };
			pids.push_back( data );

			std::pair<Key, std::vector<struct pid_s>> p( key, pids );
			std::pair<std::unordered_map<Key, std::vector<struct pid_s>>::iterator, bool> r;

			r = this->degraded.keys.insert( p );

			ret = r.second;
		} else {
			isReconstructed = true;
			UNLOCK( &this->degraded.keysLock );
			return false;
		}
	} else {
		std::vector<struct pid_s> &pids = it->second;
		struct pid_s data = { .instanceId = instanceId, .requestId = requestId };

		pids.push_back( data );
		ret = false;
	}
	UNLOCK( &this->degraded.keysLock );

	return ret;
}

bool DegradedMap::deleteDegradedKey( Key key, std::vector<struct pid_s> &pids, bool success ) {
	std::unordered_map<Key, std::vector<struct pid_s>>::iterator it;

	LOCK( &this->degraded.keysLock );
	it = this->degraded.keys.find( key );
	if ( it == this->degraded.keys.end() ) {
		UNLOCK( &this->degraded.keysLock );
		return false;
	} else {
		pids = it->second;
		this->degraded.keys.erase( it );

		if ( success ) {
			Key k = key;
			k.dup();
			this->degraded.reconstructedKeys.insert( k );
		}
	}
	UNLOCK( &this->degraded.keysLock );

	return true;
}

bool DegradedMap::insertChunk( uint32_t listId, uint32_t stripeId, uint32_t chunkId, Chunk *chunk, bool isParity, bool needsLock, bool needsUnlock ) {
	Metadata metadata;
	metadata.set( listId, stripeId, chunkId );

	std::pair<Metadata, Chunk *> p( metadata, chunk );
	std::pair<std::unordered_map<Metadata, Chunk *>::iterator, bool> ret;

	if ( needsLock ) LOCK( &this->cacheLock );
	ret = this->cache.insert( p );
	// Mark the chunk as present
	LOCK ( &this->removedLock );
	this->removed.erase( metadata );
	UNLOCK ( &this->removedLock );
	if ( needsUnlock ) UNLOCK( &this->cacheLock );

	if ( ret.second && ! isParity ) {
		char *ptr = ChunkUtil::getData( chunk );
		char *keyPtr, *valuePtr;
		uint8_t keySize;
		uint32_t valueSize, offset = 0, size;

		LOCK( &this->keysLock );
		while( ptr + KEY_VALUE_METADATA_SIZE < ChunkUtil::getData( chunk ) + ChunkBuffer::capacity ) {
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
		// Mark the chunk as absent
		LOCK ( &this->removedLock );
		this->removed.insert( metadata );
		UNLOCK ( &this->removedLock );
	}
	UNLOCK( &this->cacheLock );

	return chunk;
}

bool DegradedMap::findRemovedChunk( uint32_t listId, uint32_t stripeId, uint32_t chunkId ) {
	Metadata metadata;
	bool ret = false;
	metadata.set( listId, stripeId, chunkId );
	LOCK ( &this->removedLock );
	ret = this->removed.count( metadata ) > 0;
	UNLOCK ( &this->removedLock );
	return ret;
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
			Chunk *chunk = it->second;
			Metadata metadata = ChunkUtil::getMetadata( chunk );
			fprintf(
				f, "(list ID: %u, stripe ID: %u, chunk ID: %u) --> %p (size: %u)\n",
				metadata.listId, metadata.stripeId, metadata.chunkId,
				chunk, ChunkUtil::getSize( chunk )
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

bool DegradedChunkBuffer::updateKeyValue( uint8_t keySize, char *keyStr, uint32_t valueUpdateSize, uint32_t valueUpdateOffset, uint32_t chunkUpdateOffset, char *valueUpdate, Chunk *chunk, bool isSealed, KeyValue *keyValuePtr, Metadata *metadataPtr ) {
	Key key;
	LOCK_T *keysLock, *cacheLock;
	std::unordered_map<Key, KeyMetadata> *keys;
	std::unordered_map<Metadata, Chunk *> *cache;
	bool ret = false;

	this->map.getKeysMap( keys, keysLock );
	this->map.getCacheMap( cache, cacheLock );

	key.set( keySize, keyStr );

	if ( isSealed ) {
		LOCK( cacheLock );
		ChunkUtil::computeDelta(
			chunk,
			valueUpdate, // delta
			valueUpdate, // new data
			chunkUpdateOffset,
			valueUpdateSize,
			true // perform update
		);
		UNLOCK( cacheLock );
		ret = true;
	} else {
		LOCK( &this->map.unsealed.lock );
		std::unordered_map<Key, KeyValue>::iterator it = this->map.unsealed.values.find( key );
		if ( it == this->map.unsealed.values.end() ) {
			if ( ! metadataPtr || ! keyValuePtr ) {
				UNLOCK( &this->map.unsealed.lock );
				return false;
			}

			Metadata metadata = *metadataPtr;
			KeyValue keyValue = *keyValuePtr;
			std::pair<std::unordered_map<Key, KeyValue>::iterator, bool> r;

			uint8_t tmpKeySize;
			uint32_t tmpValueSize;
			char *tmpKeyStr, *tmpValueStr;
			keyValue.deserialize( tmpKeyStr, tmpKeySize, tmpValueStr, tmpValueSize );

			keyValue.dup( tmpKeyStr, tmpKeySize, tmpValueStr, tmpValueSize );
			key = keyValue.key();

			std::pair<Key, KeyValue> p1( key, keyValue );
			std::pair<Metadata, Key> p2( metadata, key );

			r = this->map.unsealed.values.insert( p1 );
			if ( r.second )
				this->map.unsealed.metadataRev.insert( p2 );

			UNLOCK( &this->map.unsealed.lock );

			return r.second;
		} else {
			KeyValue &keyValue = it->second;
			uint32_t dataUpdateOffset = KeyValue::getChunkUpdateOffset(
				0,                // chunkOffset
				keySize,          // keySize
				valueUpdateOffset // valueUpdateOffset
			);
			memcpy(
				keyValue.data + dataUpdateOffset,
				valueUpdate,
				valueUpdateSize
			);
		}
		UNLOCK( &this->map.unsealed.lock );

		ret = true;
	}

	return ret;
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
			deltaSize = ChunkUtil::deleteObject( chunk, keyMetadata.offset, delta );
			ret = true;
		}
		UNLOCK( cacheLock );
	} else {
		ret = this->map.deleteValue( key, metadata, opcode, timestamp );
	}
	UNLOCK( keysLock );

	return ret;
}

bool DegradedChunkBuffer::update(
	uint32_t listId, uint32_t stripeId, uint32_t chunkId, uint32_t updatingChunkId,
	uint32_t offset, uint32_t size, char *dataDelta,
	Chunk **dataChunks, Chunk *dataChunk, Chunk *parityChunk
) {
	Metadata metadata;
	metadata.set( listId, stripeId, updatingChunkId );

	LOCK_T *cacheLock;
	std::unordered_map<Metadata, Chunk *> *cache;
	std::unordered_map<Metadata, Chunk *>::iterator it;
	Chunk *chunk;
	this->map.getCacheMap( cache, cacheLock );

	// Prepare data delta
	ChunkUtil::clear( dataChunk );
	ChunkUtil::copy( dataChunk, offset, dataDelta, size );

	// Prepare the stripe
	for ( uint32_t i = 0; i < ChunkBuffer::dataChunkCount; i++ )
		dataChunks[ i ] = Coding::zeros;
	dataChunks[ chunkId ] = dataChunk;

	ChunkUtil::clear( parityChunk );

	// Compute parity delta
	ChunkBuffer::coding->encode(
		dataChunks, parityChunk, updatingChunkId - ChunkBuffer::dataChunkCount + 1,
		offset + chunkId * ChunkBuffer::capacity,
		offset + chunkId * ChunkBuffer::capacity + size
	);

	// Update the parity chunk
	LOCK( cacheLock );
	it = cache->find( metadata );
	if ( it == cache->end() ) {
		// Allocate new chunk
		chunk = this->tempChunkPool.alloc();
		ChunkUtil::set( chunk, listId, stripeId, updatingChunkId );

		std::pair<Metadata, Chunk *> p( metadata, chunk );
		cache->insert( p );
	} else {
		chunk = it->second;
	}
	char *parity = ChunkUtil::getData( chunk );
	Coding::bitwiseXOR(
		parity,
		parity,
		ChunkUtil::getData( parityChunk ),
		ChunkBuffer::capacity
	);
	UNLOCK( cacheLock );

	return true;
}

DegradedChunkBuffer::~DegradedChunkBuffer() {}

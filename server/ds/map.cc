#include "map.hh"

Map::Map() {
	LOCK_INIT( &this->keysLock );
	LOCK_INIT( &this->cacheLock );
	LOCK_INIT( &this->chunksLock );
	LOCK_INIT( &this->stripeIdsLock );
	LOCK_INIT( &this->forwarded.lock );
	LOCK_INIT( &this->opsLock );
	LOCK_INIT( &this->sealedLock );
}

void Map::setTimestamp( Timestamp *timestamp ) {
	this->timestamp = timestamp;
}

bool Map::insertKey(
	Key key, uint8_t opcode, uint32_t &timestamp,
	KeyMetadata &keyMetadata,
	bool needsLock, bool needsUnlock,
	bool needsUpdateOpMetadata
) {
	key.dup();

	std::pair<Key, KeyMetadata> keyPair( key, keyMetadata );
	std::pair<std::unordered_map<Key, KeyMetadata>::iterator, bool> keyRet;

	if ( needsLock ) LOCK( &this->keysLock );

	if ( ! this->_keys.insert( key.data, key.size, keyMetadata.obj ) ) {
		if ( needsUnlock ) UNLOCK( &this->keysLock );
		return false;
	}
	///// vvvvv Deprecated vvvvv /////
	keyRet = this->keys.insert( keyPair );
	if ( ! keyRet.second ) {
		if ( needsUnlock ) UNLOCK( &this->keysLock );
		return false;
	}
	///// ^^^^^ Deprecated ^^^^^ /////
	if ( needsUnlock ) UNLOCK( &this->keysLock );

	return needsUpdateOpMetadata ? this->insertOpMetadata( opcode, timestamp, key, keyMetadata ) : true;
}

char *Map::findObject(
	char *keyStr, uint8_t keySize,
	KeyValue *keyValuePtr,
	Key *keyPtr,
	bool needsLock, bool needsUnlock
) {
	char *ret = 0;

	if ( needsLock ) LOCK( &this->keysLock );
	ret = this->_keys.find( keyStr, keySize );
	if ( needsUnlock ) UNLOCK( &this->keysLock );

	if ( keyValuePtr ) {
		if ( ret )
			keyValuePtr->set( ret );
		else
			keyValuePtr->clear();
	}
	if ( keyPtr ) {
		if ( ret ) {
			KeyValue keyValue;
			keyValue.set( ret );
			*keyPtr = keyValue.key();
		} else {
			keyPtr->set( keySize, keyStr );
		}
	}

	return ret;
}

bool Map::findValueByKey(
	char *keyStr, uint8_t keySize,
	KeyValue *keyValuePtr,
	Key *keyPtr
) {
	Key key;
	KeyValue keyValue;

	if ( keyValuePtr )
		keyValuePtr->clear();
	key.set( keySize, keyStr );

	LOCK( &this->keysLock );
	keyValue.data = this->_keys.find( keyStr, keySize );
	if ( ! keyValue.data ) {
		UNLOCK( &this->keysLock );
		if ( keyPtr ) *keyPtr = key;
		return false;
	}
	key = keyValue.key();
	if ( keyPtr ) *keyPtr = key;

	///// vvvvv Deprecated vvvvv /////
	std::unordered_map<Key, KeyMetadata>::iterator keysIt;
	keysIt = this->keys.find( key );
	if ( keysIt == this->keys.end() ) {
		UNLOCK( &this->keysLock );
		if ( keyPtr ) *keyPtr = key;
		return false;
	}

	if ( keyPtr ) *keyPtr = keysIt->first;
	///// ^^^^^ Deprecated ^^^^^ /////

	if ( keyValuePtr )
		keyValuePtr->set( keysIt->second.obj );

	UNLOCK( &this->keysLock );

	return true;
}

/*
bool Map::findValueByKey(
	char *keyStr, uint8_t keySize,
	KeyValue *keyValuePtr,
	Key *keyPtr,
	KeyMetadata *keyMetadataPtr,
	Metadata *metadataPtr,
	Chunk **chunkPtr,
	bool needsLock, bool needsUnlock
) {
	std::unordered_map<Key, KeyMetadata>::iterator keysIt;
	std::unordered_map<Metadata, Chunk *>::iterator cacheIt;
	Key key;

	if ( keyValuePtr )
		keyValuePtr->clear();
	key.set( keySize, keyStr );

	if ( needsLock ) LOCK( &this->keysLock );
	keysIt = this->keys.find( key );
	if ( keysIt == this->keys.end() ) {
		if ( needsUnlock ) UNLOCK( &this->keysLock );
		if ( keyPtr ) *keyPtr = key;
		return false;
	}

	if ( keyPtr ) *keyPtr = keysIt->first;
	if ( keyMetadataPtr ) *keyMetadataPtr = keysIt->second;
	if ( needsUnlock ) UNLOCK( &this->keysLock );

	if ( needsLock ) LOCK( &this->cacheLock );
	cacheIt = this->cache.find( keysIt->second );
	if ( cacheIt == this->cache.end() ) {
		if ( needsUnlock ) UNLOCK( &this->cacheLock );
		return false;
	}

	if ( metadataPtr ) *metadataPtr = cacheIt->first;
	if ( chunkPtr ) *chunkPtr = cacheIt->second;

	Chunk *chunk = cacheIt->second;
	if ( keyValuePtr )
		*keyValuePtr = ChunkUtil::getObject( chunk, keysIt->second.offset );
	if ( needsUnlock ) UNLOCK( &this->cacheLock );
	return true;
}
*/

bool Map::deleteKey(
	Key key, uint8_t opcode, uint32_t &timestamp,
	KeyMetadata &keyMetadata,
	bool needsLock, bool needsUnlock,
	bool needsUpdateOpMetadata
) {
	Key k;
	std::unordered_map<Key, KeyMetadata>::iterator keysIt;
	std::unordered_map<Key, OpMetadata>::iterator opsIt;

	if ( needsLock ) LOCK( &this->keysLock );
	keysIt = this->keys.find( key );
	if ( keysIt == this->keys.end() ) {
		if ( needsUnlock ) UNLOCK( &this->keysLock );
		return false;
	} else {
		k = keysIt->first;
		keyMetadata = keysIt->second;
		this->keys.erase( keysIt );
		k.free();
	}
	if ( needsUnlock ) UNLOCK( &this->keysLock );

	return needsUpdateOpMetadata ? this->insertOpMetadata( opcode, timestamp, key, keyMetadata ) : true;
}

void Map::getKeysMap( std::unordered_map<Key, KeyMetadata> *&keys, LOCK_T *&lock ) {
	keys = &this->keys;
	lock = &this->keysLock;
}

void Map::setChunk(
	uint32_t listId, uint32_t stripeId, uint32_t chunkId,
	Chunk *chunk, bool isParity,
	bool needsLock, bool needsUnlock
) {
	Metadata metadata;
	metadata.set( listId, stripeId, chunkId );

	if ( needsLock ) LOCK( &this->chunksLock );
	if ( this->chunks.find( ( char * ) chunk, CHUNK_IDENTIFIER_SIZE ) ) {
		__ERROR__( "Map", "setChunk", "This chunk (%u, %u, %u) already exists.", listId, stripeId, chunkId );
	} else {
		this->chunks.insert( ( char * ) chunk, CHUNK_IDENTIFIER_SIZE, ( char * ) chunk );
	}
	if ( needsUnlock ) UNLOCK( &this->chunksLock );

	// vvvvv Deprecated vvvvv //
	if ( needsLock ) LOCK( &this->cacheLock );
	if ( this->cache.find( metadata ) != this->cache.end() ) {
		__ERROR__( "Map", "setChunk", "This chunk (%u, %u, %u) already exists.", listId, stripeId, chunkId );
	}
	this->cache[ metadata ] = chunk;
	if ( needsUnlock ) UNLOCK( &this->cacheLock );
	// ^^^^^ Deprecated ^^^^^ //

	if ( needsLock ) LOCK( &this->stripeIdsLock );
	this->stripeIds[ listId ].insert( stripeId );
	if ( needsUnlock ) UNLOCK( &this->stripeIdsLock );

	if ( ! isParity ) {
		char *ptr = ChunkUtil::getData( chunk );
		char *keyPtr, *valuePtr;
		uint8_t keySize;
		uint32_t valueSize, offset = 0, size;

		if ( needsLock ) LOCK( &this->keysLock );
		while( ptr < ChunkUtil::getData( chunk ) + ChunkUtil::chunkSize ) {
			KeyValue::deserialize( ptr, keyPtr, keySize, valuePtr, valueSize );
			if ( keySize == 0 && valueSize == 0 )
				break;

			Key key;
			KeyMetadata keyMetadata;

			size = KEY_VALUE_METADATA_SIZE + keySize + valueSize;

			key.set( keySize, keyPtr );
			keyMetadata.set( listId, stripeId, chunkId );
			keyMetadata.offset = offset;
			keyMetadata.length = size;

			offset += size;

			this->keys[ key ] = keyMetadata;

			ptr += size;
		}
		if ( needsUnlock ) UNLOCK( &this->keysLock );
	}
}

Chunk *Map::findChunkById(
	uint32_t listId, uint32_t stripeId, uint32_t chunkId,
	Metadata *metadataPtr,
	bool needsLock, bool needsUnlock, LOCK_T **lock
) {
	Chunk *chunk = 0;
	ChunkIdentifier id( listId, stripeId, chunkId );

	if ( lock ) *lock = &this->chunksLock;
	if ( needsLock ) LOCK( &this->chunksLock );
	chunk = ( Chunk * ) this->chunks.find( ( char * ) &id, CHUNK_IDENTIFIER_SIZE );
	if ( ! chunk ) {
		if ( needsUnlock ) UNLOCK( &this->chunksLock );
		return 0;
	}
	if ( needsUnlock ) UNLOCK( &this->chunksLock );

	return chunk;

	/*
	// vvvvv Deprecated vvvvv //
	std::unordered_map<Metadata, Chunk *>::iterator it;
	Metadata metadata;
	metadata.set( listId, stripeId, chunkId );
	if ( metadataPtr ) *metadataPtr = metadata;

	if ( lock ) *lock = &this->cacheLock;

	if ( needsLock ) LOCK( &this->cacheLock );
	it = this->cache.find( metadata );
	if ( it == this->cache.end() ) {
		if ( needsUnlock ) UNLOCK( &this->cacheLock );
		return 0;
	}
	if ( needsUnlock ) UNLOCK( &this->cacheLock );
	// ^^^^^ Deprecated ^^^^^ //
	return it->second;
	*/
}

bool Map::seal( uint32_t listId, uint32_t stripeId, uint32_t chunkId ) {
	Metadata metadata;
	metadata.set( listId, stripeId, chunkId );

	std::pair<std::unordered_set<Metadata>::iterator, bool> ret;

	LOCK( &this->sealedLock );
	ret = this->sealed.insert( metadata );
	UNLOCK( &this->sealedLock );

	return ret.second;
}

uint32_t Map::nextStripeID( uint32_t listId, uint32_t from ) {
	uint32_t ret = from;
	LOCK( &this->stripeIdsLock );
	std::unordered_set<uint32_t> &sids = this->stripeIds[ listId ];
	std::unordered_set<uint32_t>::iterator end = sids.end();
	while ( sids.find( ret ) != end )
		ret++;
	UNLOCK( &this->stripeIdsLock );
	return ret;
}

void Map::getCacheMap( std::unordered_map<Metadata, Chunk *> *&cache, LOCK_T *&lock ) {
	cache = &this->cache;
	lock = &this->cacheLock;
}

bool Map::insertOpMetadata( uint8_t opcode, uint32_t &timestamp, Key key, KeyMetadata keyMetadata, bool dup ) {
	bool ret;
	std::unordered_map<Key, OpMetadata>::iterator opsIt;
	std::pair<std::unordered_map<Key, OpMetadata>::iterator, bool> opRet;

	LOCK( &this->opsLock );
	opsIt = this->ops.find( key );
	if ( opsIt == this->ops.end() ) {
		OpMetadata opMetadata;
		opMetadata.clone( keyMetadata );
		opMetadata.opcode = opcode;
		opMetadata.timestamp = this->timestamp->nextVal();
		timestamp = opMetadata.timestamp;

		if ( dup ) key.dup();

		std::pair<Key, OpMetadata> opsPair( key, opMetadata );
		std::pair<std::unordered_map<Key, OpMetadata>::iterator, bool> opsRet;

		opsRet = this->ops.insert( opsPair );
		ret = opsRet.second;
	} else if ( opcode == PROTO_OPCODE_DELETE ) {
		Key k = opsIt->first;
		this->ops.erase( opsIt );
		k.free();
		ret = true;
	} else {
		ret = false;
	}
	UNLOCK( &this->opsLock );

	return ret;
}

bool Map::insertForwardedChunk(
	uint32_t srcListId, uint32_t srcStripeId, uint32_t srcChunkId,
	uint32_t dstListId, uint32_t dstStripeId, uint32_t dstChunkId
) {
	Metadata srcMetadata, dstMetadata;
	srcMetadata.set( srcListId, srcStripeId, srcChunkId );
	dstMetadata.set( dstListId, dstStripeId, dstChunkId );

	std::pair<std::unordered_map<Metadata, Metadata>::iterator, bool> ret;
	std::pair<Metadata, Metadata> p( srcMetadata, dstMetadata );

	LOCK( &this->forwarded.lock );
	ret = this->forwarded.chunks.insert( p );
	UNLOCK( &this->forwarded.lock );

	return ret.second;
}

bool Map::findForwardedChunk( uint32_t srcListId, uint32_t srcStripeId, uint32_t srcChunkId, Metadata &dstMetadata ) {
	Metadata srcMetadata;
	srcMetadata.set( srcListId, srcStripeId, srcChunkId );

	std::unordered_map<Metadata, Metadata>::iterator it;
	bool ret = false;

	LOCK( &this->forwarded.lock );
	it = this->forwarded.chunks.find( srcMetadata );
	if ( it != this->forwarded.chunks.end() ) {
		dstMetadata = it->second;
		ret = true;
	}
	UNLOCK( &this->forwarded.lock );

	return ret;
}

bool Map::eraseForwardedChunk( uint32_t srcListId, uint32_t srcStripeId, uint32_t srcChunkId ) {
	Metadata srcMetadata;
	srcMetadata.set( srcListId, srcStripeId, srcChunkId );

	std::unordered_map<Metadata, Metadata>::iterator it;
	bool ret;

	LOCK( &this->forwarded.lock );
	ret = this->forwarded.chunks.erase( srcMetadata ) > 0;
	UNLOCK( &this->forwarded.lock );

	return ret;
}

bool Map::insertForwardedKey(
	uint8_t keySize, char *keyStr,
	uint32_t dstListId, uint32_t dstChunkId
) {
	Metadata dstMetadata;
	Key key;

	key.set( keySize, keyStr );
	dstMetadata.set( dstListId, -1, dstChunkId );

	std::pair<std::unordered_map<Key, Metadata>::iterator, bool> ret;

	LOCK( &this->forwarded.lock );
	if ( this->forwarded.keys.find( key ) == this->forwarded.keys.end() )
		key.dup();

	std::pair<Key, Metadata> p( key, dstMetadata );
	ret = this->forwarded.keys.insert( p );
	UNLOCK( &this->forwarded.lock );

	return ret.second;
}

bool Map::findForwardedKey( uint8_t keySize, char *keyStr, Metadata &dstMetadata ) {
	Key key;
	key.set( keySize, keyStr );

	std::unordered_map<Key, Metadata>::iterator it;
	bool ret = false;

	LOCK( &this->forwarded.lock );
	it = this->forwarded.keys.find( key );
	if ( it != this->forwarded.keys.end() ) {
		dstMetadata = it->second;
		ret = true;
	}
	UNLOCK( &this->forwarded.lock );

	return ret;
}

bool Map::eraseForwardedKey( uint8_t keySize, char *keyStr ) {
	Key key;
	key.set( keySize, keyStr );

	std::unordered_map<Key, Metadata>::iterator it;
	bool ret;

	LOCK( &this->forwarded.lock );
	ret = this->forwarded.keys.erase( key ) > 0;
	UNLOCK( &this->forwarded.lock );

	return ret;
}

void Map::dump( FILE *f ) {
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

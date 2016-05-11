#include "map.hh"

Map::Map() {
	LOCK_INIT( &this->keysLock );
	LOCK_INIT( &this->chunksLock );
	LOCK_INIT( &this->stripeIdsLock );
	LOCK_INIT( &this->forwarded.lock );
	LOCK_INIT( &this->opsLock );
	LOCK_INIT( &this->sealedLock );

	this->keys.setKeySize( 0 );
	this->chunks.setKeySize( CHUNK_IDENTIFIER_SIZE );
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

	if ( needsLock ) LOCK( &this->keysLock );

	if ( ! this->keys.insert( key.data, key.size, keyMetadata.obj ) ) {
		if ( needsUnlock ) UNLOCK( &this->keysLock );
		return false;
	}
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
	ret = this->keys.find( keyStr, keySize );
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

bool Map::deleteKey(
	Key key, uint8_t opcode, uint32_t &timestamp,
	KeyMetadata &keyMetadata,
	bool needsLock, bool needsUnlock,
	bool needsUpdateOpMetadata
) {
	std::unordered_map<Key, OpMetadata>::iterator opsIt;

	if ( needsLock ) LOCK( &this->keysLock );
	if ( this->keys.find( key.data, key.size ) ) {
		this->keys.del( key.data, key.size );
	} else {
		if ( needsUnlock ) UNLOCK( &this->keysLock );
		return false;
	}
	if ( needsUnlock ) UNLOCK( &this->keysLock );

	return needsUpdateOpMetadata ? this->insertOpMetadata( opcode, timestamp, key, keyMetadata ) : true;
}

void Map::getKeysMap( CuckooHash **keys, LOCK_T **lock ) {
	if ( keys ) *keys = &this->keys;
	if ( lock ) *lock = &this->keysLock;
}

void Map::setChunk(
	uint32_t listId, uint32_t stripeId, uint32_t chunkId,
	Chunk *chunk, bool isParity,
	bool needsLock, bool needsUnlock
) {
	if ( needsLock ) LOCK( &this->chunksLock );
	if ( this->chunks.find( ( char * ) chunk, CHUNK_IDENTIFIER_SIZE ) ) {
		__ERROR__( "Map", "setChunk", "This chunk (%u, %u, %u) already exists.", listId, stripeId, chunkId );
	} else {
		this->chunks.insert( ( char * ) chunk, CHUNK_IDENTIFIER_SIZE, ( char * ) chunk );
	}
	if ( needsUnlock ) UNLOCK( &this->chunksLock );

	if ( needsLock ) LOCK( &this->stripeIdsLock );
	this->stripeIds[ listId ].insert( stripeId );
	if ( needsUnlock ) UNLOCK( &this->stripeIdsLock );

	if ( ! isParity ) {
		char *ptr = ChunkUtil::getData( chunk );
		char *keyPtr, *valuePtr;
		uint8_t keySize;
		uint32_t valueSize, offset = 0, size, splitSize, splitOffset;
		bool isLarge;

		if ( needsLock ) LOCK( &this->keysLock );
		while( ptr + KEY_VALUE_METADATA_SIZE < ChunkUtil::getData( chunk ) + ChunkUtil::chunkSize ) {
			KeyValue::deserialize( ptr, keyPtr, keySize, valuePtr, valueSize, splitOffset );
			if ( keySize == 0 && valueSize == 0 )
				break;

			isLarge = LargeObjectUtil::isLarge( keySize, valueSize, 0, &splitSize );
			if ( isLarge ) {
				if ( splitOffset + splitSize > valueSize )
					splitSize = valueSize - splitOffset;

				size = KEY_VALUE_METADATA_SIZE + SPLIT_OFFSET_SIZE + keySize + splitSize;
			} else {
				size = KEY_VALUE_METADATA_SIZE + keySize + valueSize;
			}


			this->keys.insert( keyPtr, keySize, ptr );
			offset += size;
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

void Map::getChunksMap( CuckooHash **chunks, LOCK_T **lock ) {
	if ( chunks ) *chunks = &this->chunks;
	if ( lock ) *lock = &this->chunksLock;
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

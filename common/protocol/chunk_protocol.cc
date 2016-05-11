#include "protocol.hh"

size_t Protocol::generateChunkHeader( uint8_t magic, uint8_t to, uint8_t opcode, uint16_t instanceId, uint32_t requestId, uint32_t listId, uint32_t stripeId, uint32_t chunkId, char *sendBuf ) {
	if ( ! sendBuf ) sendBuf = this->buffer.send;
	char *buf = sendBuf + PROTO_HEADER_SIZE;
	size_t bytes = this->generateHeader( magic, to, opcode, PROTO_CHUNK_SIZE, instanceId, requestId, sendBuf );

	*( ( uint32_t * )( buf      ) ) = htonl( listId );
	*( ( uint32_t * )( buf +  4 ) ) = htonl( stripeId );
	*( ( uint32_t * )( buf +  8 ) ) = htonl( chunkId );

	bytes += PROTO_CHUNK_SIZE;

	return bytes;
}

bool Protocol::parseChunkHeader( size_t offset, uint32_t &listId, uint32_t &stripeId, uint32_t &chunkId, char *buf, size_t size ) {
	if ( size - offset < PROTO_CHUNK_SIZE )
		return false;

	char *ptr = buf + offset;
	listId   = ntohl( *( ( uint32_t * )( ptr      ) ) );
	stripeId = ntohl( *( ( uint32_t * )( ptr +  4 ) ) );
	chunkId  = ntohl( *( ( uint32_t * )( ptr +  8 ) ) );

	return true;
}

bool Protocol::parseChunkHeader( struct ChunkHeader &header, char *buf, size_t size, size_t offset ) {
	if ( ! buf || ! size ) {
		buf = this->buffer.recv;
		size = this->buffer.size;
	}
	return this->parseChunkHeader(
		offset,
		header.listId,
		header.stripeId,
		header.chunkId,
		buf, size
	);
}

size_t Protocol::generateChunksHeader( uint8_t magic, uint8_t to, uint8_t opcode, uint16_t instanceId, uint32_t requestId, uint32_t listId, uint32_t chunkId, std::vector<uint32_t> &stripeIds, uint32_t &count ) {
	char *buf = this->buffer.send + PROTO_HEADER_SIZE;
	size_t bytes = PROTO_HEADER_SIZE;
	uint32_t *tmp;

	count = 0;

	*( ( uint32_t * )( buf     ) ) = htonl( listId );
	*( ( uint32_t * )( buf + 4 ) ) = htonl( chunkId );
	tmp = ( uint32_t * )( buf + 8 );

	buf += PROTO_CHUNKS_SIZE;
	bytes += PROTO_CHUNKS_SIZE;

	for ( size_t i = 0, len = stripeIds.size(); i < len; i++ ) {
		if ( bytes + 4 > this->buffer.size ) // no more space
			break;
		*( ( uint32_t * )( buf ) ) = htonl( stripeIds[ i ] );
		buf += 4;
		bytes += 4;
		count++;
	}

	*tmp = htonl( count );
	this->generateHeader( magic, to, opcode, bytes - PROTO_HEADER_SIZE, instanceId, requestId );

	return bytes;
}

bool Protocol::parseChunksHeader( size_t offset, uint32_t &listId, uint32_t &chunkId, uint32_t &numStripes, uint32_t *&stripeIds, char *buf, size_t size ) {
	if ( size - offset < PROTO_CHUNKS_SIZE )
		return false;

	char *ptr = buf + offset;
	listId     = ntohl( *( ( uint32_t * )( ptr     ) ) );
	chunkId    = ntohl( *( ( uint32_t * )( ptr + 4 ) ) );
	numStripes = ntohl( *( ( uint32_t * )( ptr + 8 ) ) );

	ptr += PROTO_CHUNKS_SIZE;

	if ( size - offset < PROTO_CHUNKS_SIZE + numStripes * 4 )
		return false;

	stripeIds = ( uint32_t * ) ptr;

	for ( uint32_t i = 0; i < numStripes; i++ )
		stripeIds[ i ] = ntohl( stripeIds[ i ] );

	return true;
}

bool Protocol::parseChunksHeader( struct ChunksHeader &header, char *buf, size_t size, size_t offset ) {
	if ( ! buf || ! size ) {
		buf = this->buffer.recv;
		size = this->buffer.size;
	}
	return this->parseChunksHeader(
		offset,
		header.listId,
		header.chunkId,
		header.numStripes,
		header.stripeIds,
		buf, size
	);
}

size_t Protocol::generateChunkDataHeader( uint8_t magic, uint8_t to, uint8_t opcode, uint16_t instanceId, uint32_t requestId, uint32_t listId, uint32_t stripeId, uint32_t chunkId, uint32_t chunkSize, uint32_t chunkOffset, char *chunkData, uint8_t sealIndicatorCount, bool *sealIndicator ) {
	char *buf = this->buffer.send + PROTO_HEADER_SIZE;
	size_t bytes = this->generateHeader( magic, to, opcode, PROTO_CHUNK_DATA_SIZE + chunkSize + sealIndicatorCount, instanceId, requestId );

	buf[ 0 ] = sealIndicatorCount;
	buf++;
	bytes += 1;

	*( ( uint32_t * )( buf      ) ) = htonl( listId );
	*( ( uint32_t * )( buf +  4 ) ) = htonl( stripeId );
	*( ( uint32_t * )( buf +  8 ) ) = htonl( chunkId );
	*( ( uint32_t * )( buf + 12 ) ) = htonl( chunkSize );
	*( ( uint32_t * )( buf + 16 ) ) = htonl( chunkOffset );
	buf += 20;
	bytes += 20;

	if ( sealIndicatorCount ) {
		for ( uint8_t i = 0; i < sealIndicatorCount; i++ )
			buf[ i ] = sealIndicator[ i ];
		buf += sealIndicatorCount;
		bytes += sealIndicatorCount;
	}

	if ( chunkSize && chunkData )
		memmove( buf, chunkData, chunkSize );
	bytes += chunkSize;

	return bytes;
}

bool Protocol::parseChunkDataHeader( size_t offset, uint32_t &listId, uint32_t &stripeId, uint32_t &chunkId, uint32_t &chunkSize, uint32_t &chunkOffset, char *&chunkData, uint8_t &sealIndicatorCount, bool *&sealIndicator, char *buf, size_t size ) {
	if ( size - offset < PROTO_CHUNK_DATA_SIZE )
		return false;

	char *ptr = buf + offset;

	sealIndicatorCount = ptr[ 0 ];
	ptr++;

	listId      = ntohl( *( ( uint32_t * )( ptr      ) ) );
	stripeId    = ntohl( *( ( uint32_t * )( ptr +  4 ) ) );
	chunkId     = ntohl( *( ( uint32_t * )( ptr +  8 ) ) );
	chunkSize   = ntohl( *( ( uint32_t * )( ptr + 12 ) ) );
	chunkOffset = ntohl( *( ( uint32_t * )( ptr + 16 ) ) );
	ptr += 20;

	if ( sealIndicatorCount ) {
		sealIndicator = ( bool * ) ptr;
		ptr += sealIndicatorCount;
	}

	if ( size - offset < PROTO_CHUNK_DATA_SIZE + sealIndicatorCount + chunkSize )
		return false;

	chunkData = chunkSize ? ptr : 0;

	return true;
}

bool Protocol::parseChunkDataHeader( struct ChunkDataHeader &header, char *buf, size_t size, size_t offset ) {
	if ( ! buf || ! size ) {
		buf = this->buffer.recv;
		size = this->buffer.size;
	}
	return this->parseChunkDataHeader(
		offset,
		header.listId,
		header.stripeId,
		header.chunkId,
		header.size,
		header.offset,
		header.data,
		header.sealIndicatorCount,
		header.sealIndicator,
		buf, size
	);
}

size_t Protocol::generateChunkKeyValueHeader(
	uint8_t magic, uint8_t to, uint8_t opcode, uint16_t instanceId, uint32_t requestId,
	uint32_t listId, uint32_t stripeId, uint32_t chunkId,
	std::unordered_map<Key, KeyValue> *values,
	std::unordered_multimap<Metadata, Key> *metadataRev,
	std::unordered_set<Key> *deleted,
	LOCK_T *lock,
	bool &isCompleted
) {
	char *buf = this->buffer.send + PROTO_HEADER_SIZE + PROTO_CHUNK_KEY_VALUE_SIZE;
	size_t bytes = PROTO_HEADER_SIZE + PROTO_CHUNK_KEY_VALUE_SIZE;
	uint32_t numValues = 0, numDeleted = 0;

	Metadata metadata;
	Key key;
	KeyValue keyValue;
	uint8_t keySize;
	uint32_t valueSize, splitOffset, splitSize;
	char *keyStr, *valueStr;
	unsigned char *tmp;

	metadata.set( listId, stripeId, chunkId );
	isCompleted = true;

	LOCK( lock );
	std::pair<
		std::unordered_multimap<Metadata, Key>::iterator,
		std::unordered_multimap<Metadata, Key>::iterator
	> mp;
	std::unordered_multimap<Metadata, Key>::iterator current, it;
	std::unordered_map<Key, KeyValue>::iterator keyValueIt;
	std::unordered_set<Key>::iterator deletedIt;

	// Deleted keys
	mp = metadataRev->equal_range( metadata );
	for ( it = mp.first; it != mp.second; ) {
		key = it->second;
		current = it;
		it++;

		deletedIt = deleted->find( key );
		if ( deletedIt != deleted->end() ) {
			key = *deletedIt;

			if ( this->buffer.size >= bytes + PROTO_KEY_SIZE + key.size ) {
				buf[ 0 ] = key.size;
				memmove( buf + 1, key.data, key.size );

				buf += PROTO_KEY_SIZE + key.size;
				bytes += PROTO_KEY_SIZE + key.size;

				deleted->erase( deletedIt );
				metadataRev->erase( current );

				key.free();

				numDeleted++;
			} else {
				isCompleted = false;
				break;
			}
		}
	}

	// Updated key-value pairs
	mp = metadataRev->equal_range( metadata );
	for ( it = mp.first; it != mp.second; ) {
		key = it->second;
		current = it;
		it++;

		keyValueIt = values->find( key );
		if ( keyValueIt != values->end() ) {
			keyValue = keyValueIt->second;
			keyValue.deserialize( keyStr, keySize, valueStr, valueSize, splitOffset );

			bool isLarge = LargeObjectUtil::isLarge( keySize, valueSize, 0, &splitSize );
			uint32_t objSize;

			if ( isLarge ) {
				if ( splitOffset + splitSize > valueSize )
					splitSize = valueSize - splitOffset;
				objSize = PROTO_KEY_VALUE_SIZE + PROTO_SPLIT_OFFSET_SIZE + keySize + valueSize;
			} else {
				objSize = PROTO_KEY_VALUE_SIZE + keySize + valueSize;
			}

			if ( this->buffer.size >= bytes + objSize ) {
				buf[ 0 ] = key.size;

				valueSize = htonl( valueSize );
				tmp = ( unsigned char * ) &valueSize;
				buf[ 1 ] = tmp[ 1 ];
				buf[ 2 ] = tmp[ 2 ];
				buf[ 3 ] = tmp[ 3 ];
				valueSize = ntohl( valueSize );

				if ( isLarge ) {
					splitSize = htonl( splitSize );
					tmp = ( unsigned char * ) &splitSize;
					buf[ 4 ] = tmp[ 1 ];
					buf[ 5 ] = tmp[ 2 ];
					buf[ 6 ] = tmp[ 3 ];
					splitSize = ntohl( splitSize );

					buf += PROTO_KEY_VALUE_SIZE + PROTO_SPLIT_OFFSET_SIZE;

					memmove( buf, keyStr, keySize );
					memmove( buf + keySize, valueStr, splitSize );

					buf += keySize + splitSize;

					bytes += PROTO_KEY_VALUE_SIZE + PROTO_SPLIT_OFFSET_SIZE + keySize + splitSize;
				} else {
					memmove( buf + 4, keyStr, keySize );
					memmove( buf + 4 + keySize, valueStr, valueSize );

					buf += PROTO_KEY_VALUE_SIZE + keySize + valueSize;
					bytes += PROTO_KEY_VALUE_SIZE + keySize + valueSize;
				}

				values->erase( keyValueIt );
				metadataRev->erase( current );

				keyValue.free();

				numValues++;
			} else {
				isCompleted = false;
				break;
			}
		}
	}

	UNLOCK( lock );

	buf = this->buffer.send + PROTO_HEADER_SIZE;

	this->generateHeader( magic, to, opcode, bytes - PROTO_HEADER_SIZE, instanceId, requestId );

	*( ( uint32_t * )( buf      ) ) = htonl( listId );
	*( ( uint32_t * )( buf +  4 ) ) = htonl( stripeId );
	*( ( uint32_t * )( buf +  8 ) ) = htonl( chunkId );
	*( ( uint32_t * )( buf + 12 ) ) = htonl( numDeleted );
	*( ( uint32_t * )( buf + 16 ) ) = htonl( numValues );
	buf[ 20 ] = isCompleted;

	return bytes;
}

bool Protocol::parseChunkKeyValueHeader( size_t offset, uint32_t &listId, uint32_t &stripeId, uint32_t &chunkId, uint32_t &deleted, uint32_t &count, bool &isCompleted, char *&dataPtr, char *buf, size_t size ) {
	if ( size - offset < PROTO_CHUNK_KEY_VALUE_SIZE )
		return false;

	char *ptr = buf + offset;
	listId   = ntohl( *( ( uint32_t * )( ptr      ) ) );
	stripeId = ntohl( *( ( uint32_t * )( ptr +  4 ) ) );
	chunkId  = ntohl( *( ( uint32_t * )( ptr +  8 ) ) );
	deleted  = ntohl( *( ( uint32_t * )( ptr + 12 ) ) );
	count    = ntohl( *( ( uint32_t * )( ptr + 16 ) ) );
	isCompleted = ptr[ 20 ];

	dataPtr = ptr + PROTO_CHUNK_KEY_VALUE_SIZE;

	return true;
}

bool Protocol::parseChunkKeyValueHeader( struct ChunkKeyValueHeader &header, char *&ptr, char *buf, size_t size, size_t offset ) {
	if ( ! buf || ! size ) {
		buf = this->buffer.recv;
		size = this->buffer.size;
	}
	return this->parseChunkKeyValueHeader(
		offset,
		header.listId,
		header.stripeId,
		header.chunkId,
		header.deleted,
		header.count,
		header.isCompleted,
		ptr,
		buf, size
	);
}

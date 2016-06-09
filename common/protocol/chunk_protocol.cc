#include "protocol.hh"

size_t Protocol::generateChunkHeader( uint8_t magic, uint8_t to, uint8_t opcode, uint16_t instanceId, uint32_t requestId, uint32_t listId, uint32_t stripeId, uint32_t chunkId, char *sendBuf ) {
	if ( ! sendBuf ) sendBuf = this->buffer.send;
	char *buf = sendBuf + PROTO_HEADER_SIZE;
	size_t bytes = this->generateHeader( magic, to, opcode, PROTO_CHUNK_SIZE, instanceId, requestId, sendBuf );
	bytes += ProtocolUtil::write4Bytes( buf, listId   );
	bytes += ProtocolUtil::write4Bytes( buf, stripeId );
	bytes += ProtocolUtil::write4Bytes( buf, chunkId  );
	return bytes;
}

bool Protocol::parseChunkHeader( struct ChunkHeader &header, char *buf, size_t size, size_t offset ) {
	if ( ! buf || ! size ) {
		buf = this->buffer.recv;
		size = this->buffer.size;
	}
	if ( size - offset < PROTO_CHUNK_SIZE ) return false;
	char *ptr = buf + offset;
	header.listId   = ProtocolUtil::read4Bytes( ptr );
	header.stripeId = ProtocolUtil::read4Bytes( ptr );
	header.chunkId  = ProtocolUtil::read4Bytes( ptr );
	return true;
}

size_t Protocol::generateChunksHeader( uint8_t magic, uint8_t to, uint8_t opcode, uint16_t instanceId, uint32_t requestId, uint32_t listId, uint32_t chunkId, std::vector<uint32_t> &stripeIds, uint32_t &count ) {
	char *buf = this->buffer.send + PROTO_HEADER_SIZE;
	size_t bytes = PROTO_HEADER_SIZE;
	uint32_t *numStripesPtr;

	count = 0;
	bytes += ProtocolUtil::write4Bytes( buf, listId  );
	bytes += ProtocolUtil::write4Bytes( buf, chunkId );
	numStripesPtr = ( uint32_t * ) buf;
	buf   += PROTO_CHUNKS_SIZE - 8;
	bytes += PROTO_CHUNKS_SIZE - 8;

	for ( size_t i = 0, len = stripeIds.size(); i < len; i++, count++ ) {
		if ( bytes + 4 > this->buffer.size ) break; // no more space
		bytes += ProtocolUtil::write4Bytes( buf, stripeIds[ i ] );
	}

	*numStripesPtr = htonl( count );
	this->generateHeader( magic, to, opcode, bytes - PROTO_HEADER_SIZE, instanceId, requestId );
	return bytes;
}

bool Protocol::parseChunksHeader( struct ChunksHeader &header, char *buf, size_t size, size_t offset ) {
	if ( ! buf || ! size ) {
		buf = this->buffer.recv;
		size = this->buffer.size;
	}
	if ( size - offset < PROTO_CHUNKS_SIZE ) return false;
	char *ptr = buf + offset;
	header.listId     = ProtocolUtil::read4Bytes( ptr );
	header.chunkId    = ProtocolUtil::read4Bytes( ptr );
	header.numStripes = ProtocolUtil::read4Bytes( ptr );

	if ( size - offset < PROTO_CHUNKS_SIZE + header.numStripes * 4 ) return false;
	header.stripeIds = ( uint32_t * ) ptr;
	for ( uint32_t i = 0; i < header.numStripes; i++ )
		header.stripeIds[ i ] = ntohl( header.stripeIds[ i ] );

	return true;
}

size_t Protocol::generateChunkDataHeader( uint8_t magic, uint8_t to, uint8_t opcode, uint16_t instanceId, uint32_t requestId, uint32_t listId, uint32_t stripeId, uint32_t chunkId, uint32_t chunkSize, uint32_t chunkOffset, char *chunkData, uint8_t sealIndicatorCount, bool *sealIndicator ) {
	char *buf = this->buffer.send + PROTO_HEADER_SIZE;
	size_t bytes = this->generateHeader( magic, to, opcode, PROTO_CHUNK_DATA_SIZE + chunkSize + sealIndicatorCount, instanceId, requestId );

	bytes += ProtocolUtil::write1Byte ( buf, sealIndicatorCount );
	bytes += ProtocolUtil::write4Bytes( buf, listId             );
	bytes += ProtocolUtil::write4Bytes( buf, stripeId           );
	bytes += ProtocolUtil::write4Bytes( buf, chunkId            );
	bytes += ProtocolUtil::write4Bytes( buf, chunkSize          );
	bytes += ProtocolUtil::write4Bytes( buf, chunkOffset        );

	if ( sealIndicatorCount )
		for ( uint8_t i = 0; i < sealIndicatorCount; i++ )
			bytes += ProtocolUtil::write1Byte( buf, sealIndicator[ i ] );
	if ( chunkSize && chunkData )
		bytes += ProtocolUtil::write( buf, chunkData, chunkSize );
	return bytes;
}

bool Protocol::parseChunkDataHeader( struct ChunkDataHeader &header, char *buf, size_t size, size_t offset ) {
	if ( ! buf || ! size ) {
		buf = this->buffer.recv;
		size = this->buffer.size;
	}
	if ( size - offset < PROTO_CHUNK_DATA_SIZE ) return false;
	char *ptr = buf + offset;

	header.sealIndicatorCount = ProtocolUtil::read1Byte ( ptr );
	header.listId             = ProtocolUtil::read4Bytes( ptr );
	header.stripeId           = ProtocolUtil::read4Bytes( ptr );
	header.chunkId            = ProtocolUtil::read4Bytes( ptr );
	header.size               = ProtocolUtil::read4Bytes( ptr );
	header.offset             = ProtocolUtil::read4Bytes( ptr );
	if ( header.sealIndicatorCount ) {
		header.sealIndicator = ( bool * ) ptr;
		ptr += header.sealIndicatorCount;
	}
	header.data = header.size ? ptr : 0;
	return ( size - offset >= PROTO_CHUNK_DATA_SIZE + header.sealIndicatorCount + header.size );
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
				bytes += ProtocolUtil::write1Byte( buf, key.size );
				bytes += ProtocolUtil::write( buf, key.data, key.size );
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

			if ( this->buffer.size >= bytes + PROTO_KEY_VALUE_SIZE + keySize + valueSize ) {
				bytes += ProtocolUtil::write1Byte ( buf, keySize       );
				bytes += ProtocolUtil::write3Bytes( buf, valueSize     );
				bytes += ProtocolUtil::write( buf, keyStr, keySize     );
				bytes += ProtocolUtil::write( buf, valueStr, valueSize );
			}

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
				bytes += ProtocolUtil::write1Byte ( buf, keySize       );
				bytes += ProtocolUtil::write3Bytes( buf, valueSize     );

				if ( isLarge ) {
					bytes += ProtocolUtil::write( buf, keyStr, keySize );
					bytes += ProtocolUtil::write3Bytes( buf, splitSize );
					bytes += ProtocolUtil::write( buf, valueStr, splitSize );
				} else {
					bytes += ProtocolUtil::write( buf, keyStr, keySize     );
					bytes += ProtocolUtil::write( buf, valueStr, valueSize );
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
	ProtocolUtil::write4Bytes( buf, listId      );
	ProtocolUtil::write4Bytes( buf, stripeId    );
	ProtocolUtil::write4Bytes( buf, chunkId     );
	ProtocolUtil::write4Bytes( buf, numDeleted  );
	ProtocolUtil::write4Bytes( buf, numValues   );
	ProtocolUtil::write1Byte ( buf, isCompleted );
	return bytes;
}

bool Protocol::parseChunkKeyValueHeader( struct ChunkKeyValueHeader &header, char *&dataPtr, char *buf, size_t size, size_t offset ) {
	if ( ! buf || ! size ) {
		buf = this->buffer.recv;
		size = this->buffer.size;
	}
	if ( size - offset < PROTO_CHUNK_KEY_VALUE_SIZE ) return false;
	char *ptr = buf + offset;
	header.listId      = ProtocolUtil::read4Bytes( ptr );
	header.stripeId    = ProtocolUtil::read4Bytes( ptr );
	header.chunkId     = ProtocolUtil::read4Bytes( ptr );
	header.deleted     = ProtocolUtil::read4Bytes( ptr );
	header.count       = ProtocolUtil::read4Bytes( ptr );
	header.isCompleted = ProtocolUtil::read1Byte( ptr );
	dataPtr            = ptr;
	return true;
}

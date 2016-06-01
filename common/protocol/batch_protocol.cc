#include "protocol.hh"

size_t Protocol::generateBatchChunkHeader(
	uint8_t magic, uint8_t to, uint8_t opcode,
	uint16_t instanceId, uint32_t requestId,
	std::vector<uint32_t> *requestIds,
	std::vector<Metadata> *metadata,
	uint32_t &chunksCount,
	bool &isCompleted
) {
	char *buf = this->buffer.send + PROTO_HEADER_SIZE;
	size_t bytes = PROTO_HEADER_SIZE;
	uint32_t *chunksCountPtr = ( uint32_t * ) buf;

	buf += PROTO_BATCH_CHUNK_SIZE;
	bytes += PROTO_BATCH_CHUNK_SIZE;

	isCompleted = true;
	chunksCount = 0;

	size_t current, len;
	for ( current = 0, len = metadata->size(); current < len; current++ ) {
		if ( this->buffer.size >= bytes + PROTO_CHUNK_SIZE + 4 ) {
			bytes += ProtocolUtil::write4Bytes( buf, requestIds->at( current )        );
			bytes += ProtocolUtil::write4Bytes( buf, metadata->at( current ).listId   );
			bytes += ProtocolUtil::write4Bytes( buf, metadata->at( current ).stripeId );
			bytes += ProtocolUtil::write4Bytes( buf, metadata->at( current ).chunkId  );
			chunksCount++;
		} else {
			isCompleted = false;
			break;
		}
	}

	if ( current == len ) {
		// All sent
		delete requestIds;
		delete metadata;
	} else {
		requestIds->erase( requestIds->begin(), requestIds->begin() + current );
		metadata->erase( metadata->begin(), metadata->begin() + current );
	}

	*chunksCountPtr = htonl( chunksCount );
	this->generateHeader( magic, to, opcode, bytes - PROTO_HEADER_SIZE, instanceId, requestId );
	return bytes;
}

bool Protocol::parseBatchChunkHeader( struct BatchChunkHeader &header, char *buf, size_t size, size_t offset ) {
	if ( ! buf || ! size ) {
		buf = this->buffer.recv;
		size = this->buffer.size;
	}
	if ( size - offset < PROTO_BATCH_CHUNK_SIZE ) return false;
	char *ptr = buf + offset;
	header.count  = ProtocolUtil::read4Bytes( ptr );
	header.chunks = ptr;
	return true;
}

bool Protocol::nextChunkInBatchChunkHeader( struct BatchChunkHeader &header, uint32_t &responseId, struct ChunkHeader &chunkHeader, uint32_t size, uint32_t &offset ) {
	char *ptr = header.chunks + offset;

	responseId = ProtocolUtil::read4Bytes( ptr );
	offset += 4;

	bool ret = this->parseChunkHeader(
		chunkHeader,
		header.chunks,
		size,
		offset
	);
	offset += PROTO_CHUNK_SIZE;
	return ret;
}

size_t Protocol::generateBatchKeyHeader(
	uint8_t magic, uint8_t to, uint8_t opcode,
	uint16_t instanceId, uint32_t requestId,
	std::unordered_set<Key> &keys, std::unordered_set<Key>::iterator &it, uint32_t &keysCount,
	bool &isCompleted
) {
	char *buf = this->buffer.send + PROTO_HEADER_SIZE;
	size_t bytes = PROTO_HEADER_SIZE;
	uint32_t *keysCountPtr = ( uint32_t * ) buf;

	buf += PROTO_BATCH_KEY_SIZE;
	bytes += PROTO_BATCH_KEY_SIZE;

	isCompleted = true;
	keysCount = 0;

	for ( ; it != keys.end(); it++ ) {
		const Key &key = *it;
		if ( this->buffer.size >= bytes + PROTO_KEY_SIZE + key.size ) {
			bytes += ProtocolUtil::write1Byte( buf, key.size );
			bytes += ProtocolUtil::write( buf, key.data, key.size );
			keysCount++;
		} else {
			isCompleted = false;
			break;
		}
	}

	*keysCountPtr = htonl( keysCount );
	this->generateHeader( magic, to, opcode, bytes - PROTO_HEADER_SIZE, instanceId, requestId );
	return bytes;
}

size_t Protocol::generateBatchKeyHeader(
	uint8_t magic, uint8_t to, uint8_t opcode,
	uint16_t instanceId, uint32_t requestId,
	struct BatchKeyValueHeader &header
) {
	char *buf = this->buffer.send + PROTO_HEADER_SIZE;
	size_t bytes = PROTO_HEADER_SIZE;
	uint32_t *keysCountPtr = ( uint32_t * ) buf;
	uint32_t keysCount = 0;

	buf += PROTO_BATCH_KEY_SIZE;
	bytes += PROTO_BATCH_KEY_SIZE;

	for ( uint32_t i = 0, offset = 0; i < header.count; i++ ) {
		uint8_t keySize;
		uint32_t valueSize;
		char *keyStr, *valueStr;
		this->nextKeyValueInBatchKeyValueHeader( header, keySize, valueSize, keyStr, valueStr, offset );
		bytes += ProtocolUtil::write1Byte( buf, keySize );
		bytes += ProtocolUtil::write( buf, keyStr, keySize );
		keysCount++;
	}

	*keysCountPtr = htonl( keysCount );
	this->generateHeader( magic, to, opcode, bytes - PROTO_HEADER_SIZE, instanceId, requestId );
	return bytes;
}

bool Protocol::parseBatchKeyHeader( struct BatchKeyHeader &header, char *buf, size_t size, size_t offset ) {
	if ( ! buf || ! size ) {
		buf = this->buffer.recv;
		size = this->buffer.size;
	}
	if ( size - offset < PROTO_BATCH_KEY_SIZE ) return false;
	char *ptr = buf + offset;
	header.count = ProtocolUtil::read4Bytes( ptr );
	header.keys  = ptr;
	return true;
}

void Protocol::nextKeyInBatchKeyHeader( struct BatchKeyHeader &header, uint8_t &keySize, char *&key, uint32_t &offset ) {
	char *ptr = header.keys + offset;
	keySize = ProtocolUtil::read1Byte( ptr );
	key = ptr;
	offset += PROTO_KEY_SIZE + keySize;
}

size_t Protocol::generateBatchKeyValueHeader(
	uint8_t magic, uint8_t to, uint8_t opcode,
	uint16_t instanceId, uint32_t requestId,
	std::unordered_set<Key> &keys, std::unordered_set<Key>::iterator &it,
	std::unordered_map<Key, KeyValue> *values, LOCK_T *lock,
	uint32_t &keyValuesCount,
	bool &isCompleted
) {
	char *buf = this->buffer.send + PROTO_HEADER_SIZE;
	size_t bytes = PROTO_HEADER_SIZE;
	uint32_t keyValueSize;
	uint32_t *keyValuesCountPtr = ( uint32_t * ) buf;
	std::unordered_map<Key, KeyValue>::iterator valuesIt;

	buf += PROTO_BATCH_KEY_VALUE_SIZE;
	bytes += PROTO_BATCH_KEY_VALUE_SIZE;

	isCompleted = true;
	keyValuesCount = 0;

	LOCK( lock );
	for ( ; it != keys.end(); it++ ) {
		const Key &key = *it;

		valuesIt = values->find( key );
		if ( valuesIt == values->end() ) {
			printf( "...key not found: (%u) %.*s...\n", key.size, key.size, key.data );
			continue;
		}

		const KeyValue &keyValue = valuesIt->second;
		keyValueSize = keyValue.getSize();

		if ( this->buffer.size >= bytes + keyValueSize ) {
			bytes += ProtocolUtil::write( buf, keyValue.data, keyValueSize );
			keyValuesCount++;
		} else {
			isCompleted = false;
			break;
		}
	}
	UNLOCK( lock );

	*keyValuesCountPtr = htonl( keyValuesCount );
	this->generateHeader( magic, to, opcode, bytes - PROTO_HEADER_SIZE, instanceId, requestId );
	return bytes;
}

bool Protocol::parseBatchKeyValueHeader( struct BatchKeyValueHeader &header, char *buf, size_t size, size_t offset ) {
	if ( ! buf || ! size ) {
		buf = this->buffer.recv;
		size = this->buffer.size;
	}
	if ( size - offset < PROTO_BATCH_KEY_VALUE_SIZE ) return false;
	char *ptr = buf + offset;
	header.count     = ProtocolUtil::read4Bytes( ptr );
	header.keyValues = ptr;
	return true;
}

void Protocol::nextKeyValueInBatchKeyValueHeader( struct BatchKeyValueHeader &header, uint8_t &keySize, uint32_t &valueSize, char *&key, char *&value, uint32_t &offset ) {
	char *ptr = header.keyValues + offset;
	keySize   = ProtocolUtil::read1Byte( ptr );
	valueSize = ProtocolUtil::read3Bytes( ptr );
	key = ptr;
	value = valueSize ? key + keySize : 0;
	offset += PROTO_KEY_VALUE_SIZE + keySize + valueSize;
}

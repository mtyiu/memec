#include "protocol.hh"

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
			buf[ 0 ] = key.size;
			memcpy( buf + PROTO_KEY_SIZE, key.data, key.size );

			buf += PROTO_KEY_SIZE + key.size;
			bytes += PROTO_KEY_SIZE + key.size;

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

	buf += PROTO_BATCH_KEY_SIZE;
	bytes += PROTO_BATCH_KEY_SIZE;

	uint32_t keysCount = 0;

	for ( uint32_t i = 0, offset = 0; i < header.count; i++ ) {
		uint8_t keySize;
		uint32_t valueSize;
		char *keyStr, *valueStr;

		this->nextKeyValueInBatchKeyValueHeader( header, keySize, valueSize, keyStr, valueStr, offset );

		buf[ 0 ] = keySize;
		memcpy( buf + PROTO_KEY_SIZE, keyStr, keySize );

		buf += PROTO_KEY_SIZE + keySize;
		bytes += PROTO_KEY_SIZE + keySize;

		keysCount++;
	}

	*keysCountPtr = htonl( keysCount );

	this->generateHeader( magic, to, opcode, bytes - PROTO_HEADER_SIZE, instanceId, requestId );

	return bytes;
}

bool Protocol::parseBatchKeyHeader( size_t offset, uint32_t &count, char *&keys, char *buf, size_t size ) {
	if ( size - offset < PROTO_BATCH_KEY_SIZE )
		return false;

	char *ptr = buf + offset;
	count = ntohl( *( ( uint32_t * )( ptr ) ) );
	keys = ptr + PROTO_BATCH_KEY_SIZE;

	return true;
}

bool Protocol::parseBatchKeyHeader( struct BatchKeyHeader &header, char *buf, size_t size, size_t offset ) {
	if ( ! buf || ! size ) {
		buf = this->buffer.recv;
		size = this->buffer.size;
	}
	return this->parseBatchKeyHeader(
		offset,
		header.count,
		header.keys,
		buf, size
	);
}

void Protocol::nextKeyInBatchKeyHeader( struct BatchKeyHeader &header, uint8_t &keySize, char *&key, uint32_t &offset ) {
	char *ptr = header.keys + offset;
	keySize = ( uint8_t ) ptr[ 0 ];
	key = ptr + PROTO_KEY_SIZE;
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
			memcpy( buf, keyValue.data, keyValueSize );

			buf += keyValueSize;
			bytes += keyValueSize;

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

bool Protocol::parseBatchKeyValueHeader( size_t offset, uint32_t &count, char *&keyValues, char *buf, size_t size ) {
	if ( size - offset < PROTO_BATCH_KEY_VALUE_SIZE )
		return false;

	char *ptr = buf + offset;
	count = ntohl( *( ( uint32_t * )( ptr ) ) );
	keyValues = ptr + PROTO_BATCH_KEY_VALUE_SIZE;

	return true;
}

bool Protocol::parseBatchKeyValueHeader( struct BatchKeyValueHeader &header, char *buf, size_t size, size_t offset ) {
	if ( ! buf || ! size ) {
		buf = this->buffer.recv;
		size = this->buffer.size;
	}
	return this->parseBatchKeyValueHeader(
		offset,
		header.count,
		header.keyValues,
		buf, size
	);
}

void Protocol::nextKeyValueInBatchKeyValueHeader( struct BatchKeyValueHeader &header, uint8_t &keySize, uint32_t &valueSize, char *&key, char *&value, uint32_t &offset ) {
	char *ptr = header.keyValues + offset;
	unsigned char *tmp;

	keySize = ( uint8_t ) ptr[ 0 ];
	valueSize = 0;
	tmp = ( unsigned char * ) &valueSize;
	tmp[ 1 ] = ptr[ 1 ];
	tmp[ 2 ] = ptr[ 2 ];
	tmp[ 3 ] = ptr[ 3 ];
	valueSize = ntohl( valueSize );

	key = ptr + PROTO_KEY_VALUE_SIZE;
	value = valueSize ? key + keySize : 0;

	offset += PROTO_KEY_VALUE_SIZE + keySize + valueSize;
}

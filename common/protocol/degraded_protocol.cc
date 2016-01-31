#include "protocol.hh"

size_t Protocol::generateDegradedLockReqHeader(
	uint8_t magic, uint8_t to, uint8_t opcode, uint16_t instanceId, uint32_t requestId,
	uint32_t *original, uint32_t *reconstructed, uint32_t reconstructedCount,
	uint8_t keySize, char *key
) {
	char *buf = this->buffer.send + PROTO_HEADER_SIZE;
	size_t bytes = this->generateHeader(
		magic, to, opcode,
		PROTO_DEGRADED_LOCK_REQ_SIZE + keySize + reconstructedCount * 4 * 4,
		instanceId, requestId
	);

	*( ( uint32_t * )( buf      ) ) = htonl( reconstructedCount );
	buf[ 4 ] = keySize;

	buf += PROTO_DEGRADED_LOCK_REQ_SIZE;
	bytes += PROTO_DEGRADED_LOCK_REQ_SIZE;

	memmove( buf, key, keySize );
	buf += keySize;
	bytes += keySize;

	for ( uint32_t i = 0; i < reconstructedCount; i++ ) {
		*( ( uint32_t * )( buf     ) ) = htonl( original[ i * 2     ] );
		*( ( uint32_t * )( buf + 4 ) ) = htonl( original[ i * 2 + 1 ] );
		buf += 8;
		bytes += 8;
	}
	for ( uint32_t i = 0; i < reconstructedCount; i++ ) {
		*( ( uint32_t * )( buf     ) ) = htonl( reconstructed[ i * 2     ] );
		*( ( uint32_t * )( buf + 4 ) ) = htonl( reconstructed[ i * 2 + 1 ] );
		buf += 8;
		bytes += 8;
	}

	return bytes;
}

bool Protocol::parseDegradedLockReqHeader(
	size_t offset,
	uint32_t *&original, uint32_t *&reconstructed, uint32_t &reconstructedCount,
	uint8_t &keySize, char *&key, char *buf, size_t size
) {
	if ( size - offset < PROTO_DEGRADED_LOCK_REQ_SIZE )
		return false;

	char *ptr = buf + offset;
	reconstructedCount = ntohl( *( ( uint32_t * )( ptr      ) ) );
	keySize = ptr[ 4 ];

	if ( size - offset < ( size_t ) PROTO_DEGRADED_LOCK_REQ_SIZE + keySize + reconstructedCount * 4 * 4 )
		return false;

	key = ptr + PROTO_DEGRADED_LOCK_REQ_SIZE;
	ptr += PROTO_DEGRADED_LOCK_REQ_SIZE + keySize;

	original = ( uint32_t * ) ptr;
	reconstructed = ( ( uint32_t * ) ptr ) + reconstructedCount * 2;
	for ( uint32_t i = 0; i < reconstructedCount; i++ ) {
		original[ i * 2     ] = ntohl( original[ i * 2     ] );
		original[ i * 2 + 1 ] = ntohl( original[ i * 2 + 1 ] );
		reconstructed[ i * 2     ] = ntohl( reconstructed[ i * 2     ] );
		reconstructed[ i * 2 + 1 ] = ntohl( reconstructed[ i * 2 + 1 ] );
	}

	return true;
}

bool Protocol::parseDegradedLockReqHeader( struct DegradedLockReqHeader &header, char *buf, size_t size, size_t offset ) {
	if ( ! buf || ! size ) {
		buf = this->buffer.recv;
		size = this->buffer.size;
	}
	return this->parseDegradedLockReqHeader(
		offset,
		header.original,
		header.reconstructed,
		header.reconstructedCount,
		header.keySize,
		header.key,
		buf, size
	);
}

size_t Protocol::generateDegradedLockResHeader( uint8_t magic, uint8_t to, uint8_t opcode, uint16_t instanceId, uint32_t requestId, uint32_t length, uint8_t type, uint8_t keySize, char *key, char *&buf ) {
	buf = this->buffer.send + PROTO_HEADER_SIZE;
	size_t bytes = this->generateHeader(
		magic, to, opcode,
		PROTO_DEGRADED_LOCK_RES_BASE_SIZE + keySize + length,
		instanceId, requestId
	);

	buf[ 0 ] = type;
	buf[ 1 ] = keySize;

	buf += PROTO_DEGRADED_LOCK_RES_BASE_SIZE;
	bytes += PROTO_DEGRADED_LOCK_RES_BASE_SIZE;

	memmove( buf, key, keySize );
	buf += keySize;
	bytes += keySize;

	return bytes;
}

size_t Protocol::generateDegradedLockResHeader(
	uint8_t magic, uint8_t to, uint8_t opcode, uint16_t instanceId, uint32_t requestId,
	bool isLocked, uint8_t keySize, char *key,
	bool isSealed, uint32_t stripeId, uint32_t dataChunkId, uint32_t dataChunkCount,
	uint32_t *original, uint32_t *reconstructed, uint32_t reconstructedCount,
	uint32_t ongoingAtChunk
) {
	char *buf;
	size_t bytes = this->generateDegradedLockResHeader(
		magic, to, opcode, instanceId, requestId,
		PROTO_DEGRADED_LOCK_RES_LOCK_SIZE + reconstructedCount * 4 * 4,
		isLocked ? PROTO_DEGRADED_LOCK_RES_IS_LOCKED : PROTO_DEGRADED_LOCK_RES_WAS_LOCKED,
		keySize, key, buf
	);

	buf[ 0 ] = isSealed;
	buf++;
	bytes++;

	*( ( uint32_t * )( buf      ) ) = htonl( stripeId );
	*( ( uint32_t * )( buf +  4 ) ) = htonl( reconstructedCount );
	*( ( uint32_t * )( buf +  8 ) ) = htonl( ongoingAtChunk );
	buf += 12;
	bytes += 12;

	// Entries that are related to this key
	for ( uint32_t i = 0; i < reconstructedCount; i++ ) {
		uint32_t chunkId = original[ i * 2 + 1 ];
		if ( ( chunkId < dataChunkCount && chunkId == dataChunkId ) ||
		     ( chunkId >= dataChunkCount ) ) {
			*( ( uint32_t * )( buf     ) ) = htonl( original[ i * 2     ] );
			*( ( uint32_t * )( buf + 4 ) ) = htonl( original[ i * 2 + 1 ] );
			buf += 8;
			bytes += 8;
		}
	}
	// Entries that are not related to this key
	for ( uint32_t i = 0; i < reconstructedCount; i++ ) {
		uint32_t chunkId = original[ i * 2 + 1 ];
		if ( ( chunkId < dataChunkCount && chunkId == dataChunkId ) ||
		     ( chunkId >= dataChunkCount ) ) {
			// Copied
		} else {
			*( ( uint32_t * )( buf     ) ) = htonl( original[ i * 2     ] );
			*( ( uint32_t * )( buf + 4 ) ) = htonl( original[ i * 2 + 1 ] );
			buf += 8;
			bytes += 8;
		}
	}

	// Entries that are related to this key
	for ( uint32_t i = 0; i < reconstructedCount; i++ ) {
		uint32_t chunkId = original[ i * 2 + 1 ];
		if ( ( chunkId < dataChunkCount && chunkId == dataChunkId ) ||
		     ( chunkId >= dataChunkCount ) ) {
			*( ( uint32_t * )( buf     ) ) = htonl( reconstructed[ i * 2     ] );
			*( ( uint32_t * )( buf + 4 ) ) = htonl( reconstructed[ i * 2 + 1 ] );
			buf += 8;
			bytes += 8;
		}
	}
	// Entries that are not related to this key
	for ( uint32_t i = 0; i < reconstructedCount; i++ ) {
		uint32_t chunkId = original[ i * 2 + 1 ];
		if ( ( chunkId < dataChunkCount && chunkId == dataChunkId ) ||
		     ( chunkId >= dataChunkCount ) ) {
			// Copied
		} else {
			*( ( uint32_t * )( buf     ) ) = htonl( reconstructed[ i * 2     ] );
			*( ( uint32_t * )( buf + 4 ) ) = htonl( reconstructed[ i * 2 + 1 ] );
			buf += 8;
			bytes += 8;
		}
	}

	return bytes;
}

size_t Protocol::generateDegradedLockResHeader(
	uint8_t magic, uint8_t to, uint8_t opcode, uint16_t instanceId, uint32_t requestId,
	bool exist, uint8_t keySize, char *key
) {
	char *buf;
	size_t bytes = this->generateDegradedLockResHeader(
		magic, to, opcode, instanceId, requestId,
		PROTO_DEGRADED_LOCK_RES_NOT_SIZE,
		exist ? PROTO_DEGRADED_LOCK_RES_NOT_LOCKED : PROTO_DEGRADED_LOCK_RES_NOT_EXIST,
		keySize, key, buf
	);
	return bytes;
}

size_t Protocol::generateDegradedLockResHeader(
	uint8_t magic, uint8_t to, uint8_t opcode, uint16_t instanceId, uint32_t requestId,
	uint8_t keySize, char *key,
	uint32_t *original, uint32_t *remapped, uint32_t remappedCount
) {
	char *buf;
	size_t bytes = this->generateDegradedLockResHeader(
		magic, to, opcode, instanceId, requestId,
		PROTO_DEGRADED_LOCK_RES_REMAP_SIZE + remappedCount * 4 * 4,
		PROTO_DEGRADED_LOCK_RES_REMAPPED,
		keySize, key, buf
	);

	*( ( uint32_t * )( buf      ) ) = htonl( remappedCount );
	buf += 4;
	bytes += 4;

	for ( uint32_t i = 0; i < remappedCount; i++ ) {
		*( ( uint32_t * )( buf     ) ) = htonl( original[ i * 2     ] );
		*( ( uint32_t * )( buf + 4 ) ) = htonl( original[ i * 2 + 1 ] );
		buf += 8;
		bytes += 8;
	}
	for ( uint32_t i = 0; i < remappedCount; i++ ) {
		*( ( uint32_t * )( buf     ) ) = htonl( remapped[ i * 2     ] );
		*( ( uint32_t * )( buf + 4 ) ) = htonl( remapped[ i * 2 + 1 ] );
		buf += 8;
		bytes += 8;
	}

	return bytes;
}

bool Protocol::parseDegradedLockResHeader( size_t offset, uint8_t &type, uint8_t &keySize, char *&key, char *buf, size_t size ) {
	if ( size - offset < PROTO_DEGRADED_LOCK_RES_BASE_SIZE )
		return false;

	char *ptr = buf + offset;
	type = ptr[ 0 ];
	keySize = ptr[ 1 ];

	if ( size - offset < PROTO_DEGRADED_LOCK_RES_BASE_SIZE + ( size_t ) keySize )
		return false;

	key = ptr + PROTO_DEGRADED_LOCK_RES_BASE_SIZE;

	return true;
}

bool Protocol::parseDegradedLockResHeader(
	size_t offset, bool &isSealed,
	uint32_t &stripeId,
	uint32_t *&original, uint32_t *&reconstructed, uint32_t &reconstructedCount,
	uint32_t &ongoingAtChunk,
	char *buf, size_t size
) {
	if ( size - offset < PROTO_DEGRADED_LOCK_RES_LOCK_SIZE )
		return false;

	char *ptr = buf + offset;

	isSealed = ptr[ 0 ];
	ptr++;

	stripeId           = ntohl( *( ( uint32_t * )( ptr      ) ) );
	reconstructedCount = ntohl( *( ( uint32_t * )( ptr +  4 ) ) );
	ongoingAtChunk     = ntohl( *( ( uint32_t * )( ptr +  8 ) ) );
	ptr += 12;

	original = ( uint32_t * ) ptr;
	reconstructed = ( ( uint32_t * ) ptr ) + reconstructedCount * 2;
	for ( uint32_t i = 0; i < reconstructedCount; i++ ) {
		original[ i * 2     ] = ntohl( original[ i * 2     ] );
		original[ i * 2 + 1 ] = ntohl( original[ i * 2 + 1 ] );
		reconstructed[ i * 2     ] = ntohl( reconstructed[ i * 2     ] );
		reconstructed[ i * 2 + 1 ] = ntohl( reconstructed[ i * 2 + 1 ] );
	}

	return true;
}

bool Protocol::parseDegradedLockResHeader(
	size_t offset,
	uint32_t *&original, uint32_t *&remapped, uint32_t &remappedCount,
	char *buf, size_t size
) {
	if ( size - offset < PROTO_DEGRADED_LOCK_RES_REMAP_SIZE )
		return false;

	char *ptr = buf + offset;
	remappedCount = ntohl( *( ( uint32_t * )( ptr ) ) );
	ptr += 4;

	original = ( uint32_t * ) ptr;
	remapped = ( ( uint32_t * ) ptr ) + remappedCount * 2;
	for ( uint32_t i = 0; i < remappedCount; i++ ) {
		original[ i * 2     ] = ntohl( original[ i * 2     ] );
		original[ i * 2 + 1 ] = ntohl( original[ i * 2 + 1 ] );
		remapped[ i * 2     ] = ntohl( remapped[ i * 2     ] );
		remapped[ i * 2 + 1 ] = ntohl( remapped[ i * 2 + 1 ] );
	}


	return true;
}

bool Protocol::parseDegradedLockResHeader( struct DegradedLockResHeader &header, char *buf, size_t size, size_t offset ) {
	if ( ! buf || ! size ) {
		buf = this->buffer.recv;
		size = this->buffer.size;
	}
	bool ret = this->parseDegradedLockResHeader(
		offset,
		header.type,
		header.keySize,
		header.key,
		buf, size
	);
	if ( ! ret )
		return false;

	offset += PROTO_DEGRADED_LOCK_RES_BASE_SIZE + header.keySize;
	switch( header.type ) {
		case PROTO_DEGRADED_LOCK_RES_IS_LOCKED:
		case PROTO_DEGRADED_LOCK_RES_WAS_LOCKED:
			ret = this->parseDegradedLockResHeader(
				offset,
				header.isSealed,
				header.stripeId,
				header.original,
				header.reconstructed,
				header.reconstructedCount,
				header.ongoingAtChunk,
				buf, size
			);
			break;
		case PROTO_DEGRADED_LOCK_RES_NOT_LOCKED:
		case PROTO_DEGRADED_LOCK_RES_NOT_EXIST:
			break;
		case PROTO_DEGRADED_LOCK_RES_REMAPPED:
			ret = this->parseDegradedLockResHeader(
				offset,
				header.original,
				header.remapped,
				header.remappedCount,
				buf, size
			);
			break;
		default:
			return false;
	}
	return ret;
}

size_t Protocol::generateDegradedReqHeader(
	uint8_t magic, uint8_t to, uint8_t opcode, uint16_t instanceId, uint32_t requestId,
	bool isSealed, uint32_t stripeId,
	uint32_t *original, uint32_t *reconstructed, uint32_t reconstructedCount,
	uint32_t ongoingAtChunk,
	uint8_t keySize, char *key,
	uint32_t timestamp
) {
	char *buf = this->buffer.send + PROTO_HEADER_SIZE;
	size_t bytes = this->generateHeader(
		magic, to, opcode,
		PROTO_DEGRADED_REQ_BASE_SIZE + reconstructedCount * 4 * 4 + PROTO_KEY_SIZE + keySize,
		instanceId, requestId, 0,
		timestamp
	);

	buf[ 0 ] = isSealed;
	buf += 1;
	bytes += 1;

	*( ( uint32_t * )( buf      ) ) = htonl( stripeId );
	*( ( uint32_t * )( buf +  4 ) ) = htonl( reconstructedCount );
	*( ( uint32_t * )( buf +  8 ) ) = htonl( ongoingAtChunk );
	buf += 12;
	bytes += 12;

	for ( uint32_t i = 0; i < reconstructedCount; i++ ) {
		*( ( uint32_t * )( buf     ) ) = htonl( original[ i * 2     ] );
		*( ( uint32_t * )( buf + 4 ) ) = htonl( original[ i * 2 + 1 ] );
		buf += 8;
		bytes += 8;
	}
	for ( uint32_t i = 0; i < reconstructedCount; i++ ) {
		*( ( uint32_t * )( buf     ) ) = htonl( reconstructed[ i * 2     ] );
		*( ( uint32_t * )( buf + 4 ) ) = htonl( reconstructed[ i * 2 + 1 ] );
		buf += 8;
		bytes += 8;
	}

	buf[ 0 ] = keySize;
	buf += PROTO_KEY_SIZE;
	bytes += PROTO_KEY_SIZE;

	memmove( buf, key, keySize );
	bytes += keySize;

	return bytes;
}

size_t Protocol::generateDegradedReqHeader(
	uint8_t magic, uint8_t to, uint8_t opcode, uint16_t instanceId, uint32_t requestId,
	bool isSealed, uint32_t stripeId,
	uint32_t *original, uint32_t *reconstructed, uint32_t reconstructedCount,
	uint32_t ongoingAtChunk,
	uint8_t keySize, char *key,
	uint32_t valueUpdateOffset, uint32_t valueUpdateSize, char *valueUpdate,
	uint32_t timestamp
) {
	char *buf = this->buffer.send + PROTO_HEADER_SIZE;
	size_t bytes = this->generateHeader(
		magic, to, opcode,
		PROTO_DEGRADED_REQ_BASE_SIZE + reconstructedCount * 4 * 4 + PROTO_KEY_VALUE_UPDATE_SIZE + keySize + valueUpdateSize,
		instanceId, requestId,
		0,
		timestamp
	);

	buf[ 0 ] = isSealed;
	buf += 1;
	bytes += 1;

	*( ( uint32_t * )( buf      ) ) = htonl( stripeId );
	*( ( uint32_t * )( buf +  4 ) ) = htonl( reconstructedCount );
	*( ( uint32_t * )( buf +  8 ) ) = htonl( ongoingAtChunk );
	buf += 12;
	bytes += 12;

	for ( uint32_t i = 0; i < reconstructedCount; i++ ) {
		*( ( uint32_t * )( buf     ) ) = htonl( original[ i * 2     ] );
		*( ( uint32_t * )( buf + 4 ) ) = htonl( original[ i * 2 + 1 ] );
		buf += 8;
		bytes += 8;
	}
	for ( uint32_t i = 0; i < reconstructedCount; i++ ) {
		*( ( uint32_t * )( buf     ) ) = htonl( reconstructed[ i * 2     ] );
		*( ( uint32_t * )( buf + 4 ) ) = htonl( reconstructed[ i * 2 + 1 ] );
		buf += 8;
		bytes += 8;
	}

	buf[ 0 ] = keySize;
	unsigned char *tmp;
	valueUpdateSize = htonl( valueUpdateSize );
	valueUpdateOffset = htonl( valueUpdateOffset );
	tmp = ( unsigned char * ) &valueUpdateSize;
	buf[ 1 ] = tmp[ 1 ];
	buf[ 2 ] = tmp[ 2 ];
	buf[ 3 ] = tmp[ 3 ];
	tmp = ( unsigned char * ) &valueUpdateOffset;
	buf[ 4 ] = tmp[ 1 ];
	buf[ 5 ] = tmp[ 2 ];
	buf[ 6 ] = tmp[ 3 ];
	valueUpdateSize = ntohl( valueUpdateSize );
	valueUpdateOffset = ntohl( valueUpdateOffset );

	buf += PROTO_KEY_VALUE_UPDATE_SIZE;
	bytes += PROTO_KEY_VALUE_UPDATE_SIZE;

	memmove( buf, key, keySize );
	buf += keySize;
	bytes += keySize;

	memmove( buf, valueUpdate, valueUpdateSize );
	bytes += valueUpdateSize;

	return bytes;
}

bool Protocol::parseDegradedReqHeader(
	size_t offset,
	bool &isSealed, uint32_t &stripeId,
	uint32_t *&original, uint32_t *&reconstructed, uint32_t &reconstructedCount,
	uint32_t &ongoingAtChunk,
	char *buf, size_t size
) {
	if ( size - offset < PROTO_DEGRADED_REQ_BASE_SIZE )
		return false;

	char *ptr = buf + offset;

	isSealed = ptr[ 0 ];
	ptr += 1;

	stripeId           = ntohl( *( ( uint32_t * )( ptr      ) ) );
	reconstructedCount = ntohl( *( ( uint32_t * )( ptr +  4 ) ) );
	ongoingAtChunk     = ntohl( *( ( uint32_t * )( ptr +  8 ) ) );
	ptr += 12;

	original = ( uint32_t * ) ptr;
	reconstructed = ( ( uint32_t * ) ptr ) + reconstructedCount * 2;
	for ( uint32_t i = 0; i < reconstructedCount; i++ ) {
		original[ i * 2     ] = ntohl( original[ i * 2     ] );
		original[ i * 2 + 1 ] = ntohl( original[ i * 2 + 1 ] );
		reconstructed[ i * 2     ] = ntohl( reconstructed[ i * 2     ] );
		reconstructed[ i * 2 + 1 ] = ntohl( reconstructed[ i * 2 + 1 ] );
	}

	return true;
}

bool Protocol::parseDegradedReqHeader( struct DegradedReqHeader &header, uint8_t opcode, char *buf, size_t size, size_t offset ) {
	if ( ! buf || ! size ) {
		buf = this->buffer.recv;
		size = this->buffer.size;
	}
	bool ret = this->parseDegradedReqHeader(
		offset,
		header.isSealed,
		header.stripeId,
		header.original,
		header.reconstructed,
		header.reconstructedCount,
		header.ongoingAtChunk,
		buf, size
	);
	if ( ! ret )
		return false;
	offset += PROTO_DEGRADED_REQ_BASE_SIZE + header.reconstructedCount * 4 * 4;

	switch( opcode ) {
		case PROTO_OPCODE_DEGRADED_GET:
		case PROTO_OPCODE_DEGRADED_DELETE:
			ret = this->parseKeyHeader(
				offset,
				header.data.key.keySize,
				header.data.key.key,
				buf, size
			);
			break;
		case PROTO_OPCODE_DEGRADED_UPDATE:
			ret = this->parseKeyValueUpdateHeader(
				offset,
				header.data.keyValueUpdate.keySize,
				header.data.keyValueUpdate.key,
				header.data.keyValueUpdate.valueUpdateOffset,
				header.data.keyValueUpdate.valueUpdateSize,
				header.data.keyValueUpdate.valueUpdate,
				buf, size
			);
			break;
		default:
			return false;
	}
	return ret;
}

size_t Protocol::generateForwardKeyReqHeader(
	uint8_t magic, uint8_t to, uint8_t opcode,
	uint16_t instanceId, uint32_t requestId,
	uint8_t degradedOpcode, uint32_t listId, uint32_t stripeId, uint32_t chunkId,
	uint8_t keySize, char *key, uint32_t valueSize, char *value,
	uint32_t valueUpdateSize, uint32_t valueUpdateOffset, char *valueUpdate
) {
	char *buf = this->buffer.send + PROTO_HEADER_SIZE;
	size_t bytes = this->generateHeader(
		magic, to, opcode,
		PROTO_FORWARD_KEY_BASE_SIZE + keySize + valueSize +
			( degradedOpcode == PROTO_OPCODE_DEGRADED_UPDATE ? PROTO_FORWARD_KEY_UPDATE_SIZE + valueUpdateSize : 0 ),
		instanceId, requestId
	);

	buf[ 0 ] = degradedOpcode;
	buf += 1;
	bytes += 1;

	*( ( uint32_t * )( buf     ) ) = htonl( listId );
	*( ( uint32_t * )( buf + 4 ) ) = htonl( stripeId );
	*( ( uint32_t * )( buf + 8 ) ) = htonl( chunkId );
	buf[ 12 ] = keySize;
	buf += 13;
	bytes += 13;

	unsigned char *tmp;
	valueSize = htonl( valueSize );
	tmp = ( unsigned char * ) &valueSize;
	buf[ 0 ] = tmp[ 1 ];
	buf[ 1 ] = tmp[ 2 ];
	buf[ 2 ] = tmp[ 3 ];
	valueSize = ntohl( valueSize );
	buf += 3;
	bytes += 3;

	memmove( buf, key, keySize );
	memmove( buf + keySize, value, valueSize );
	buf += keySize + valueSize;
	bytes += keySize + valueSize;

	if ( degradedOpcode == PROTO_OPCODE_DEGRADED_UPDATE ) {
		valueUpdateSize = htonl( valueUpdateSize );
		tmp = ( unsigned char * ) &valueUpdateSize;
		buf[ 0 ] = tmp[ 1 ];
		buf[ 1 ] = tmp[ 2 ];
		buf[ 2 ] = tmp[ 3 ];
		valueUpdateSize = ntohl( valueUpdateSize );

		valueUpdateOffset = htonl( valueUpdateOffset );
		tmp = ( unsigned char * ) &valueUpdateOffset;
		buf[ 3 ] = tmp[ 1 ];
		buf[ 4 ] = tmp[ 2 ];
		buf[ 5 ] = tmp[ 3 ];
		valueUpdateOffset = ntohl( valueUpdateOffset );

		buf += PROTO_FORWARD_KEY_UPDATE_SIZE;
		bytes += PROTO_FORWARD_KEY_UPDATE_SIZE;

		memmove( buf, valueUpdate, valueUpdateSize );

		buf += valueUpdateSize;
		bytes += valueUpdateSize;
	}

	return bytes;
}

bool Protocol::parseForwardKeyReqHeader(
	size_t offset, uint8_t &opcode,
	uint32_t &listId, uint32_t &stripeId, uint32_t &chunkId,
	uint8_t &keySize, uint32_t &valueSize,
	char *&key, char *&value,
	uint32_t &valueUpdateSize, uint32_t &valueUpdateOffset, char *&valueUpdate,
	char *buf, size_t size
) {
	if ( size - offset < PROTO_FORWARD_KEY_BASE_SIZE )
		return false;

	char *ptr = buf + offset;
	unsigned char *tmp;

	opcode = ptr[ 0 ];
	ptr += 1;

	listId   = ntohl( *( ( uint32_t * )( ptr     ) ) );
	stripeId = ntohl( *( ( uint32_t * )( ptr + 4 ) ) );
	chunkId  = ntohl( *( ( uint32_t * )( ptr + 8 ) ) );
	keySize = ptr[ 12 ];
	ptr += 13;

	valueSize = 0;
	tmp = ( unsigned char * ) &valueSize;
	tmp[ 1 ] = ptr[ 0 ];
	tmp[ 2 ] = ptr[ 1 ];
	tmp[ 3 ] = ptr[ 2 ];
	valueSize = ntohl( valueSize );
	ptr += 3;

	if ( size - offset < ( size_t ) PROTO_FORWARD_KEY_BASE_SIZE + keySize + valueSize )
		return false;

	key = ptr;
	value = ptr + keySize;
	ptr += keySize + valueSize;

	if ( opcode == PROTO_OPCODE_DEGRADED_UPDATE ) {
		if ( size - offset < ( size_t ) PROTO_FORWARD_KEY_BASE_SIZE + keySize + valueSize + PROTO_FORWARD_KEY_UPDATE_SIZE )
			return false;

		valueUpdateSize = 0;
		tmp = ( unsigned char * ) &valueUpdateSize;
		tmp[ 1 ] = ptr[ 0 ];
		tmp[ 2 ] = ptr[ 1 ];
		tmp[ 3 ] = ptr[ 2 ];
		valueUpdateSize = ntohl( valueUpdateSize );

		valueUpdateOffset = 0;
		tmp = ( unsigned char * ) &valueUpdateOffset;
		tmp[ 1 ] = ptr[ 3 ];
		tmp[ 2 ] = ptr[ 4 ];
		tmp[ 3 ] = ptr[ 5 ];
		valueUpdateOffset = ntohl( valueUpdateOffset );

		ptr += PROTO_FORWARD_KEY_UPDATE_SIZE;

		if ( size - offset < ( size_t ) PROTO_FORWARD_KEY_BASE_SIZE + keySize + valueSize + PROTO_FORWARD_KEY_UPDATE_SIZE + valueUpdateSize )
			return false;

		valueUpdate = ptr;
	}

	return true;
}

bool Protocol::parseForwardKeyReqHeader( struct ForwardKeyHeader &header, char *buf, size_t size, size_t offset ) {
	if ( ! buf || ! size ) {
		buf = this->buffer.recv;
		size = this->buffer.size;
	}
	return this->parseForwardKeyReqHeader(
		offset,
		header.opcode,
		header.listId,
		header.stripeId,
		header.chunkId,
		header.keySize,
		header.valueSize,
		header.key,
		header.value,
		header.valueUpdateSize,
		header.valueUpdateOffset,
		header.valueUpdate,
		buf, size
	);
}

size_t Protocol::generateForwardKeyResHeader(
	uint8_t magic, uint8_t to, uint8_t opcode,
	uint16_t instanceId, uint32_t requestId,
	uint8_t degradedOpcode, uint32_t listId, uint32_t stripeId, uint32_t chunkId,
	uint8_t keySize, char *key, uint32_t valueSize,
	uint32_t valueUpdateSize, uint32_t valueUpdateOffset
) {
	char *buf = this->buffer.send + PROTO_HEADER_SIZE;
	size_t bytes = this->generateHeader(
		magic, to, opcode,
		PROTO_FORWARD_KEY_BASE_SIZE + keySize +
			( degradedOpcode == PROTO_OPCODE_DEGRADED_UPDATE ? PROTO_FORWARD_KEY_UPDATE_SIZE : 0 ),
		instanceId, requestId
	);

	buf[ 0 ] = degradedOpcode;
	buf += 1;
	bytes += 1;

	*( ( uint32_t * )( buf     ) ) = htonl( listId );
	*( ( uint32_t * )( buf + 4 ) ) = htonl( stripeId );
	*( ( uint32_t * )( buf + 8 ) ) = htonl( chunkId );
	buf[ 12 ] = keySize;
	buf += 13;
	bytes += 13;

	unsigned char *tmp;
	valueSize = htonl( valueSize );
	tmp = ( unsigned char * ) &valueSize;
	buf[ 0 ] = tmp[ 1 ];
	buf[ 1 ] = tmp[ 2 ];
	buf[ 2 ] = tmp[ 3 ];
	valueSize = ntohl( valueSize );
	buf += 3;
	bytes += 3;

	memmove( buf, key, keySize );
	buf += keySize;
	bytes += keySize;

	if ( degradedOpcode == PROTO_OPCODE_DEGRADED_UPDATE ) {
		valueUpdateSize = htonl( valueUpdateSize );
		tmp = ( unsigned char * ) &valueUpdateSize;
		buf[ 0 ] = tmp[ 1 ];
		buf[ 1 ] = tmp[ 2 ];
		buf[ 2 ] = tmp[ 3 ];
		valueUpdateSize = ntohl( valueUpdateSize );

		valueUpdateOffset = htonl( valueUpdateOffset );
		tmp = ( unsigned char * ) &valueUpdateOffset;
		buf[ 3 ] = tmp[ 1 ];
		buf[ 4 ] = tmp[ 2 ];
		buf[ 5 ] = tmp[ 3 ];
		valueUpdateOffset = ntohl( valueUpdateOffset );

		buf += PROTO_FORWARD_KEY_UPDATE_SIZE;
		bytes += PROTO_FORWARD_KEY_UPDATE_SIZE;
	}

	return bytes;
}

bool Protocol::parseForwardKeyResHeader(
	size_t offset, uint8_t &opcode,
	uint32_t &listId, uint32_t &stripeId, uint32_t &chunkId,
	uint8_t &keySize, uint32_t &valueSize,
	char *&key,
	uint32_t &valueUpdateSize, uint32_t &valueUpdateOffset,
	char *buf, size_t size
) {
	if ( size - offset < PROTO_FORWARD_KEY_BASE_SIZE )
		return false;

	char *ptr = buf + offset;
	unsigned char *tmp;

	opcode = ptr[ 0 ];
	ptr += 1;

	listId   = ntohl( *( ( uint32_t * )( ptr     ) ) );
	stripeId = ntohl( *( ( uint32_t * )( ptr + 4 ) ) );
	chunkId  = ntohl( *( ( uint32_t * )( ptr + 8 ) ) );
	keySize = ptr[ 12 ];
	ptr += 13;

	valueSize = 0;
	tmp = ( unsigned char * ) &valueSize;
	tmp[ 1 ] = ptr[ 0 ];
	tmp[ 2 ] = ptr[ 1 ];
	tmp[ 3 ] = ptr[ 2 ];
	valueSize = ntohl( valueSize );
	ptr += 3;

	if ( size - offset < ( size_t ) PROTO_FORWARD_KEY_BASE_SIZE + keySize )
		return false;

	key = ptr;
	ptr += keySize;

	if ( opcode == PROTO_OPCODE_DEGRADED_UPDATE ) {
		if ( size - offset < ( size_t ) PROTO_FORWARD_KEY_BASE_SIZE + keySize + PROTO_FORWARD_KEY_UPDATE_SIZE )
			return false;

		valueUpdateSize = 0;
		tmp = ( unsigned char * ) &valueUpdateSize;
		tmp[ 1 ] = ptr[ 0 ];
		tmp[ 2 ] = ptr[ 1 ];
		tmp[ 3 ] = ptr[ 2 ];
		valueUpdateSize = ntohl( valueUpdateSize );

		valueUpdateOffset = 0;
		tmp = ( unsigned char * ) &valueUpdateOffset;
		tmp[ 1 ] = ptr[ 3 ];
		tmp[ 2 ] = ptr[ 4 ];
		tmp[ 3 ] = ptr[ 5 ];
		valueUpdateOffset = ntohl( valueUpdateOffset );
	}

	return true;
}

bool Protocol::parseForwardKeyResHeader( struct ForwardKeyHeader &header, char *buf, size_t size, size_t offset ) {
	if ( ! buf || ! size ) {
		buf = this->buffer.recv;
		size = this->buffer.size;
	}
	return this->parseForwardKeyResHeader(
		offset,
		header.opcode,
		header.listId,
		header.stripeId,
		header.chunkId,
		header.keySize,
		header.valueSize,
		header.key,
		header.valueUpdateSize,
		header.valueUpdateOffset,
		buf, size
	);
}

size_t Protocol::generateListStripeKeyHeader( uint8_t magic, uint8_t to, uint8_t opcode, uint16_t instanceId, uint32_t requestId, uint32_t listId, uint32_t chunkId, uint8_t keySize, char *key ) {
	char *buf = this->buffer.send + PROTO_HEADER_SIZE;
	size_t bytes = this->generateHeader( magic, to, opcode, PROTO_LIST_STRIPE_KEY_SIZE + keySize, instanceId, requestId );

	*( ( uint32_t * )( buf     ) ) = htonl( listId );
	*( ( uint32_t * )( buf + 4 ) ) = htonl( chunkId );
	buf[ 8 ] = keySize;

	buf += PROTO_LIST_STRIPE_KEY_SIZE;
	memmove( buf, key, keySize );

	bytes += PROTO_LIST_STRIPE_KEY_SIZE + keySize;

	return bytes;
}

bool Protocol::parseListStripeKeyHeader( size_t offset, uint32_t &listId, uint32_t &chunkId, uint8_t &keySize, char *&key, char *buf, size_t size ) {
	if ( size - offset < PROTO_LIST_STRIPE_KEY_SIZE )
		return false;

	char *ptr = buf + offset;
	listId  = ntohl( *( ( uint32_t * )( ptr     ) ) );
	chunkId = ntohl( *( ( uint32_t * )( ptr + 4 ) ) );
	keySize = ptr[ 8 ];

	if ( size - offset < ( size_t ) PROTO_LIST_STRIPE_KEY_SIZE + keySize )
		return false;

	key = ptr + PROTO_LIST_STRIPE_KEY_SIZE;

	return true;
}

bool Protocol::parseListStripeKeyHeader( struct ListStripeKeyHeader &header, char *buf, size_t size, size_t offset ) {
	if ( ! buf || ! size ) {
		buf = this->buffer.recv;
		size = this->buffer.size;
	}
	return this->parseListStripeKeyHeader(
		offset,
		header.listId,
		header.chunkId,
		header.keySize,
		header.key,
		buf, size
	);
}

size_t Protocol::generateDegradedReleaseReqHeader( uint8_t magic, uint8_t to, uint8_t opcode, uint16_t instanceId, uint32_t requestId, std::vector<Metadata> &chunks, bool &isCompleted ) {
	char *buf = this->buffer.send + PROTO_HEADER_SIZE;
	size_t bytes = 0;
	uint32_t count = 0;

	isCompleted = true;

	for ( size_t i = 0, len = chunks.size(); i < len; i++ ) {
		if ( this->buffer.size >= bytes + PROTO_DEGRADED_RELEASE_REQ_SIZE ) {
			*( ( uint32_t * )( buf      ) ) = htonl( chunks[ i ].listId );
			*( ( uint32_t * )( buf +  4 ) ) = htonl( chunks[ i ].stripeId );
			*( ( uint32_t * )( buf +  8 ) ) = htonl( chunks[ i ].chunkId );

			count++;
		} else {
			isCompleted = false;
			break;
		}

		buf += PROTO_DEGRADED_RELEASE_REQ_SIZE;
		bytes += PROTO_DEGRADED_RELEASE_REQ_SIZE;
	}

	bytes += this->generateHeader( magic, to, opcode, bytes, instanceId, requestId );

	return bytes;
}

size_t Protocol::generateDegradedReleaseResHeader( uint8_t magic, uint8_t to, uint8_t opcode, uint16_t instanceId, uint32_t requestId, uint32_t count ) {
	char *buf = this->buffer.send + PROTO_HEADER_SIZE;
	size_t bytes = this->generateHeader( magic, to, opcode, PROTO_DEGRADED_RELEASE_RES_SIZE, instanceId, requestId );

	*( ( uint32_t * )( buf ) ) = htonl( count );

	bytes += PROTO_DEGRADED_RELEASE_RES_SIZE;

	return bytes;
}

bool Protocol::parseDegradedReleaseResHeader( size_t offset, uint32_t &count, char* buf, size_t size ) {
	if ( size - offset < PROTO_DEGRADED_RELEASE_RES_SIZE )
		return false;

	char *ptr = buf + offset;
	count = ntohl( *( ( uint32_t * )( ptr ) ) );

	return true;
}

bool Protocol::parseDegradedReleaseResHeader( struct DegradedReleaseResHeader &header, char *buf, size_t size, size_t offset ) {
	if ( ! buf || ! size ) {
		buf = this->buffer.recv;
		size = this->buffer.size;
	}
	return this->parseDegradedReleaseResHeader(
		offset,
		header.count,
		buf, size
	);
}

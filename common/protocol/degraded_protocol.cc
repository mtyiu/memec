#include "protocol.hh"

size_t Protocol::generateDegradedLockReqHeader( uint8_t magic, uint8_t to, uint8_t opcode, uint16_t instanceId, uint32_t requestId, uint32_t listId, uint32_t srcDataChunkId, uint32_t dstDataChunkId, uint32_t srcParityChunkId, uint32_t dstParityChunkId, uint8_t keySize, char *key ) {
	char *buf = this->buffer.send + PROTO_HEADER_SIZE;
	size_t bytes = this->generateHeader( magic, to, opcode, PROTO_DEGRADED_LOCK_REQ_SIZE + keySize, instanceId, requestId );

	*( ( uint32_t * )( buf      ) ) = htonl( listId );
	*( ( uint32_t * )( buf +  4 ) ) = htonl( srcDataChunkId );
	*( ( uint32_t * )( buf +  8 ) ) = htonl( dstDataChunkId );
	*( ( uint32_t * )( buf + 12 ) ) = htonl( srcParityChunkId );
	*( ( uint32_t * )( buf + 16 ) ) = htonl( dstParityChunkId );
	buf[ 20 ] = keySize;

	buf += PROTO_DEGRADED_LOCK_REQ_SIZE;
	memmove( buf, key, keySize );

	bytes += PROTO_DEGRADED_LOCK_REQ_SIZE + keySize;

	return bytes;
}

bool Protocol::parseDegradedLockReqHeader( size_t offset, uint32_t &listId, uint32_t &srcDataChunkId, uint32_t &dstDataChunkId, uint32_t &srcParityChunkId, uint32_t &dstParityChunkId, uint8_t &keySize, char *&key, char *buf, size_t size ) {
	if ( size - offset < PROTO_DEGRADED_LOCK_REQ_SIZE )
		return false;

	char *ptr = buf + offset;
	listId           = ntohl( *( ( uint32_t * )( ptr      ) ) );
	srcDataChunkId   = ntohl( *( ( uint32_t * )( ptr +  4 ) ) );
	dstDataChunkId   = ntohl( *( ( uint32_t * )( ptr +  8 ) ) );
	srcParityChunkId = ntohl( *( ( uint32_t * )( ptr + 12 ) ) );
	dstParityChunkId = ntohl( *( ( uint32_t * )( ptr + 16 ) ) );
	keySize = ptr[ 20 ];

	if ( size - offset < PROTO_DEGRADED_LOCK_REQ_SIZE + ( size_t ) keySize )
		return false;

	key = ptr + PROTO_DEGRADED_LOCK_REQ_SIZE;

	return true;
}

bool Protocol::parseDegradedLockReqHeader( struct DegradedLockReqHeader &header, char *buf, size_t size, size_t offset ) {
	if ( ! buf || ! size ) {
		buf = this->buffer.recv;
		size = this->buffer.size;
	}
	return this->parseDegradedLockReqHeader(
		offset,
		header.listId,
		header.srcDataChunkId,
		header.dstDataChunkId,
		header.srcParityChunkId,
		header.dstParityChunkId,
		header.keySize,
		header.key,
		buf, size
	);
}

size_t Protocol::generateDegradedLockResHeader( uint8_t magic, uint8_t to, uint8_t opcode, uint16_t instanceId, uint32_t requestId, uint8_t type, uint8_t keySize, char *key, char *&buf ) {
	uint32_t length;
	buf = this->buffer.send + PROTO_HEADER_SIZE;
	switch( type ) {
		case PROTO_DEGRADED_LOCK_RES_IS_LOCKED:
		case PROTO_DEGRADED_LOCK_RES_WAS_LOCKED:
			length = PROTO_DEGRADED_LOCK_RES_LOCK_SIZE;
			break;
		case PROTO_DEGRADED_LOCK_RES_REMAPPED:
			length = PROTO_DEGRADED_LOCK_RES_REMAP_SIZE;
			break;
		case PROTO_DEGRADED_LOCK_RES_NOT_LOCKED:
		case PROTO_DEGRADED_LOCK_RES_NOT_EXIST:
		default:
			length = PROTO_DEGRADED_LOCK_RES_NOT_SIZE;
			break;
	}
	size_t bytes = this->generateHeader(
		magic, to, opcode,
		PROTO_DEGRADED_LOCK_RES_BASE_SIZE + keySize + length,
		instanceId, requestId
	);

	buf[ 0 ] = type;
	buf[ 1 ] = keySize;

	buf += PROTO_DEGRADED_LOCK_RES_BASE_SIZE;
	memmove( buf, key, keySize );
	buf += keySize;

	bytes += PROTO_DEGRADED_LOCK_RES_BASE_SIZE + keySize;

	return bytes;
}

size_t Protocol::generateDegradedLockResHeader( uint8_t magic, uint8_t to, uint8_t opcode, uint16_t instanceId, uint32_t requestId, bool isLocked, bool isSealed, uint8_t keySize, char *key, uint32_t listId, uint32_t stripeId, uint32_t srcDataChunkId, uint32_t dstDataChunkId, uint32_t srcParityChunkId, uint32_t dstParityChunkId ) {
	char *buf;
	size_t bytes = this->generateDegradedLockResHeader(
		magic, to, opcode, instanceId, requestId,
		isLocked ? PROTO_DEGRADED_LOCK_RES_IS_LOCKED : PROTO_DEGRADED_LOCK_RES_WAS_LOCKED,
		keySize, key, buf
	);

	*( ( uint32_t * )( buf      ) ) = htonl( listId );
	*( ( uint32_t * )( buf +  4 ) ) = htonl( stripeId );
	*( ( uint32_t * )( buf +  8 ) ) = htonl( srcDataChunkId );
	*( ( uint32_t * )( buf + 12 ) ) = htonl( dstDataChunkId );
	*( ( uint32_t * )( buf + 16 ) ) = htonl( srcParityChunkId );
	*( ( uint32_t * )( buf + 20 ) ) = htonl( dstParityChunkId );
	buf[ 24 ] = isSealed;

	bytes += PROTO_DEGRADED_LOCK_RES_LOCK_SIZE;

	return bytes;
}

size_t Protocol::generateDegradedLockResHeader( uint8_t magic, uint8_t to, uint8_t opcode, uint16_t instanceId, uint32_t requestId, bool exist, uint8_t keySize, char *key, uint32_t listId, uint32_t srcDataChunkId, uint32_t srcParityChunkId ) {
	char *buf;
	size_t bytes = this->generateDegradedLockResHeader(
		magic, to, opcode, instanceId, requestId,
		exist ? PROTO_DEGRADED_LOCK_RES_NOT_LOCKED : PROTO_DEGRADED_LOCK_RES_NOT_EXIST,
		keySize, key, buf
	);

	*( ( uint32_t * )( buf     ) ) = htonl( listId );
	*( ( uint32_t * )( buf + 4 ) ) = htonl( srcDataChunkId );
	*( ( uint32_t * )( buf + 8 ) ) = htonl( srcParityChunkId );

	bytes += PROTO_DEGRADED_LOCK_RES_NOT_SIZE;

	return bytes;
}

size_t Protocol::generateDegradedLockResHeader( uint8_t magic, uint8_t to, uint8_t opcode, uint16_t instanceId, uint32_t requestId, uint8_t keySize, char *key, uint32_t listId, uint32_t srcDataChunkId, uint32_t dstDataChunkId, uint32_t srcParityChunkId, uint32_t dstParityChunkId ) {
	char *buf;
	size_t bytes = this->generateDegradedLockResHeader(
		magic, to, opcode, instanceId, requestId,
		PROTO_DEGRADED_LOCK_RES_REMAPPED,
		keySize, key, buf
	);

	*( ( uint32_t * )( buf      ) ) = htonl( listId );
	*( ( uint32_t * )( buf +  4 ) ) = htonl( srcDataChunkId );
	*( ( uint32_t * )( buf +  8 ) ) = htonl( dstDataChunkId );
	*( ( uint32_t * )( buf + 12 ) ) = htonl( srcParityChunkId );
	*( ( uint32_t * )( buf + 16 ) ) = htonl( dstParityChunkId );

	bytes += PROTO_DEGRADED_LOCK_RES_REMAP_SIZE;

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

bool Protocol::parseDegradedLockResHeader( size_t offset, uint32_t &listId, uint32_t &stripeId, uint32_t &srcDataChunkId, uint32_t &dstDataChunkId, uint32_t &srcParityChunkId, uint32_t &dstParityChunkId, bool &isSealed, char *buf, size_t size ) {
	if ( size - offset < PROTO_DEGRADED_LOCK_RES_LOCK_SIZE )
		return false;

	char *ptr = buf + offset;
	listId           = ntohl( *( ( uint32_t * )( ptr      ) ) );
	stripeId         = ntohl( *( ( uint32_t * )( ptr +  4 ) ) );
	srcDataChunkId   = ntohl( *( ( uint32_t * )( ptr +  8 ) ) );
	dstDataChunkId   = ntohl( *( ( uint32_t * )( ptr + 12 ) ) );
	srcParityChunkId = ntohl( *( ( uint32_t * )( ptr + 16 ) ) );
	dstParityChunkId = ntohl( *( ( uint32_t * )( ptr + 20 ) ) );
	isSealed = ptr[ 24 ];

	return true;
}

bool Protocol::parseDegradedLockResHeader( size_t offset, uint32_t &listId, uint32_t &srcDataChunkId, uint32_t &srcParityChunkId, char *buf, size_t size ) {
	if ( size - offset < PROTO_DEGRADED_LOCK_RES_NOT_SIZE )
		return false;

	char *ptr = buf + offset;
	listId           = ntohl( *( ( uint32_t * )( ptr     ) ) );
	srcDataChunkId   = ntohl( *( ( uint32_t * )( ptr + 4 ) ) );
	srcParityChunkId = ntohl( *( ( uint32_t * )( ptr + 8 ) ) );

	return true;
}

bool Protocol::parseDegradedLockResHeader( size_t offset, uint32_t &listId, uint32_t &srcDataChunkId, uint32_t &dstDataChunkId, uint32_t &srcParityChunkId, uint32_t &dstParityChunkId, char *buf, size_t size ) {
	if ( size - offset < PROTO_DEGRADED_LOCK_RES_REMAP_SIZE )
		return false;

	char *ptr = buf + offset;
	listId           = ntohl( *( ( uint32_t * )( ptr      ) ) );
	srcDataChunkId   = ntohl( *( ( uint32_t * )( ptr +  4 ) ) );
	dstDataChunkId   = ntohl( *( ( uint32_t * )( ptr +  8 ) ) );
	srcParityChunkId = ntohl( *( ( uint32_t * )( ptr + 12 ) ) );
	dstParityChunkId = ntohl( *( ( uint32_t * )( ptr + 16 ) ) );

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
				header.listId,
				header.stripeId,
				header.srcDataChunkId,
				header.dstDataChunkId,
				header.srcParityChunkId,
				header.dstParityChunkId,
				header.isSealed,
				buf, size
			);
			break;
		case PROTO_DEGRADED_LOCK_RES_NOT_LOCKED:
		case PROTO_DEGRADED_LOCK_RES_NOT_EXIST:
			ret = this->parseDegradedLockResHeader(
				offset,
				header.listId,
				header.srcDataChunkId,
				header.srcParityChunkId,
				buf, size
			);
			break;
		case PROTO_DEGRADED_LOCK_RES_REMAPPED:
			ret = this->parseDegradedLockResHeader(
				offset,
				header.listId,
				header.srcDataChunkId,
				header.dstDataChunkId,
				header.srcParityChunkId,
				header.dstParityChunkId,
				buf, size
			);
			break;
		default:
			return false;
	}
	return ret;
}

size_t Protocol::generateDegradedReqHeader( uint8_t magic, uint8_t to, uint8_t opcode, uint16_t instanceId, uint32_t requestId, uint32_t listId, uint32_t stripeId, uint32_t srcDataChunkId, uint32_t dstDataChunkId, uint32_t srcParityChunkId, uint32_t dstParityChunkId, bool isSealed, uint8_t keySize, char *key ) {
	char *buf = this->buffer.send + PROTO_HEADER_SIZE;
	size_t bytes = this->generateHeader(
		magic, to, opcode,
		PROTO_DEGRADED_REQ_BASE_SIZE + PROTO_KEY_SIZE + keySize,
		instanceId, requestId
	);

	*( ( uint32_t * )( buf      ) ) = htonl( listId );
	*( ( uint32_t * )( buf +  4 ) ) = htonl( stripeId );
	*( ( uint32_t * )( buf +  8 ) ) = htonl( srcDataChunkId );
	*( ( uint32_t * )( buf + 12 ) ) = htonl( dstDataChunkId );
	*( ( uint32_t * )( buf + 16 ) ) = htonl( srcParityChunkId );
	*( ( uint32_t * )( buf + 20 ) ) = htonl( dstParityChunkId );
	buf[ 24 ] = isSealed;
	buf += PROTO_DEGRADED_REQ_BASE_SIZE;

	buf[ 0 ] = keySize;

	buf += PROTO_KEY_SIZE;
	memmove( buf, key, keySize );

	bytes += PROTO_DEGRADED_REQ_BASE_SIZE + PROTO_KEY_SIZE + keySize;

	return bytes;
}

size_t Protocol::generateDegradedReqHeader( uint8_t magic, uint8_t to, uint8_t opcode, uint16_t instanceId, uint32_t requestId, uint32_t listId, uint32_t stripeId, uint32_t srcDataChunkId, uint32_t dstDataChunkId, uint32_t srcParityChunkId, uint32_t dstParityChunkId, bool isSealed, uint8_t keySize, char *key, uint32_t valueUpdateOffset, uint32_t valueUpdateSize, char *valueUpdate ) {
	char *buf = this->buffer.send + PROTO_HEADER_SIZE;
	size_t bytes = this->generateHeader(
		magic, to, opcode,
		PROTO_DEGRADED_REQ_BASE_SIZE + PROTO_KEY_VALUE_UPDATE_SIZE + keySize + valueUpdateSize,
		instanceId, requestId
	);

	*( ( uint32_t * )( buf      ) ) = htonl( listId );
	*( ( uint32_t * )( buf +  4 ) ) = htonl( stripeId );
	*( ( uint32_t * )( buf +  8 ) ) = htonl( srcDataChunkId );
	*( ( uint32_t * )( buf + 12 ) ) = htonl( dstDataChunkId );
	*( ( uint32_t * )( buf + 16 ) ) = htonl( srcParityChunkId );
	*( ( uint32_t * )( buf + 20 ) ) = htonl( dstParityChunkId );
	buf[ 24 ] = isSealed;
	buf += PROTO_DEGRADED_REQ_BASE_SIZE;

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
	memmove( buf, key, keySize );
	buf += keySize;
	memmove( buf, valueUpdate, valueUpdateSize );

	bytes += PROTO_DEGRADED_REQ_BASE_SIZE + PROTO_KEY_VALUE_UPDATE_SIZE + keySize + valueUpdateSize;

	return bytes;
}

bool Protocol::parseDegradedReqHeader( size_t offset, uint32_t &listId, uint32_t &stripeId, uint32_t &srcDataChunkId, uint32_t &dstDataChunkId, uint32_t &srcParityChunkId, uint32_t &dstParityChunkId, bool &isSealed, char *buf, size_t size ) {
	if ( size - offset < PROTO_DEGRADED_REQ_BASE_SIZE )
		return false;

	char *ptr = buf + offset;
	listId           = ntohl( *( ( uint32_t * )( ptr      ) ) );
	stripeId         = ntohl( *( ( uint32_t * )( ptr +  4 ) ) );
	srcDataChunkId   = ntohl( *( ( uint32_t * )( ptr +  8 ) ) );
	dstDataChunkId   = ntohl( *( ( uint32_t * )( ptr + 12 ) ) );
	srcParityChunkId = ntohl( *( ( uint32_t * )( ptr + 16 ) ) );
	dstParityChunkId = ntohl( *( ( uint32_t * )( ptr + 20 ) ) );
	isSealed = ptr[ 24 ];

	return true;
}

bool Protocol::parseDegradedReqHeader( struct DegradedReqHeader &header, uint8_t opcode, char *buf, size_t size, size_t offset ) {
	if ( ! buf || ! size ) {
		buf = this->buffer.recv;
		size = this->buffer.size;
	}
	bool ret = this->parseDegradedReqHeader(
		offset,
		header.listId,
		header.stripeId,
		header.srcDataChunkId,
		header.dstDataChunkId,
		header.srcParityChunkId,
		header.dstParityChunkId,
		header.isSealed,
		buf, size
	);
	if ( ! ret )
		return false;

	offset += PROTO_DEGRADED_REQ_BASE_SIZE;
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

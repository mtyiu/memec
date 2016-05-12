#include "protocol.hh"

size_t Protocol::generateRemappingLockHeader( uint8_t magic, uint8_t to, uint8_t opcode, uint16_t instanceId, uint32_t requestId, uint32_t *original, uint32_t *remapped, uint32_t remappedCount, uint8_t keySize, char *key ) {
	char *buf = this->buffer.send + PROTO_HEADER_SIZE;
	size_t bytes = this->generateHeader(
		magic, to, opcode,
		PROTO_REMAPPING_LOCK_SIZE + keySize + remappedCount * 4 * 4,
		instanceId, requestId
	);

	*( ( uint32_t * )( buf ) ) = htonl( remappedCount );
	buf[ 4 ] = keySize;
	bytes += PROTO_REMAPPING_LOCK_SIZE;
	buf += PROTO_REMAPPING_LOCK_SIZE;

	memmove( buf, key, keySize );
	buf += keySize;
	bytes += keySize;

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

bool Protocol::parseRemappingLockHeader( size_t offset, uint32_t *&original, uint32_t *&remapped, uint32_t &remappedCount, uint8_t &keySize, char *&key, char *buf, size_t size ) {
	if ( size - offset < PROTO_REMAPPING_LOCK_SIZE )
		return false;

	char *ptr = buf + offset;

	remappedCount = ntohl( *( ( uint32_t * )( ptr      ) ) );
	keySize = ptr[ 4 ];

	if ( size - offset < ( size_t ) PROTO_REMAPPING_LOCK_SIZE + keySize + remappedCount * 4 * 4 )
		return false;

	key = ptr + 5;
	ptr += PROTO_REMAPPING_LOCK_SIZE + keySize;

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

bool Protocol::parseRemappingLockHeader( struct RemappingLockHeader &header, char *buf, size_t size, size_t offset ) {
	if ( ! buf || ! size ) {
		buf = this->buffer.recv;
		size = this->buffer.size;
	}
	return this->parseRemappingLockHeader(
		offset,
		header.original,
		header.remapped,
		header.remappedCount,
		header.keySize,
		header.key,
		buf, size
	);
}

size_t Protocol::generateDegradedSetHeader( uint8_t magic, uint8_t to, uint8_t opcode, uint16_t instanceId, uint32_t requestId, uint32_t listId, uint32_t chunkId, uint32_t *original, uint32_t *remapped, uint32_t remappedCount, uint8_t keySize, char *key, uint32_t valueSize, char *value, uint32_t splitOffset, uint32_t splitSize, char *sendBuf ) {
	if ( ! sendBuf ) sendBuf = this->buffer.send;
	char *buf = sendBuf + PROTO_HEADER_SIZE;
	size_t bytes = this->generateHeader(
		magic, to, opcode,
		PROTO_DEGRADED_SET_SIZE + keySize + valueSize + remappedCount * 4 * 4,
		instanceId, requestId, sendBuf
	);

	*( ( uint32_t * )( buf     ) ) = htonl( listId );
	*( ( uint32_t * )( buf + 4 ) ) = htonl( chunkId );
	*( ( uint32_t * )( buf + 8 ) ) = htonl( remappedCount );
	bytes += 12;
	buf += 12;

	unsigned char *tmp;
	valueSize = htonl( valueSize );
	tmp = ( unsigned char * ) &valueSize;
	buf[ 0 ] = keySize;
	buf[ 1 ] = tmp[ 1 ];
	buf[ 2 ] = tmp[ 2 ];
	buf[ 3 ] = tmp[ 3 ];
	bytes += 4;
	buf += 4;
	valueSize = ntohl( valueSize );

	memmove( buf, key, keySize );
	bytes += keySize;
	buf += keySize;

	if ( splitSize == 0 || splitSize == valueSize ) {
		// No need to split
		memmove( buf, value, valueSize );
		bytes += valueSize;
		buf += valueSize;
	} else {
		// Include split offset
		splitOffset = htonl( splitOffset );
		unsigned char *tmp = ( unsigned char * ) &splitOffset;
		buf[ 0 ] = tmp[ 1 ];
		buf[ 1 ] = tmp[ 2 ];
		buf[ 2 ] = tmp[ 3 ];
		splitOffset = ntohl( splitOffset );
		bytes += PROTO_SPLIT_OFFSET_SIZE;
		buf += PROTO_SPLIT_OFFSET_SIZE;

		if ( splitOffset + splitSize > valueSize )
			splitSize = valueSize - splitOffset;

		memmove( buf, value + splitOffset, splitSize );
		bytes += splitSize;
		buf += splitSize;
	}

	if ( remappedCount ) {
		remappedCount *= 2; // Include both list ID and chunk ID
		for ( uint32_t i = 0; i < remappedCount; i++ ) {
			*( ( uint32_t * )( buf ) ) = htonl( original[ i ] );
			buf += 4;
			bytes += 4;
		}
		for ( uint32_t i = 0; i < remappedCount; i++ ) {
			*( ( uint32_t * )( buf ) ) = htonl( remapped[ i ] );
			buf += 4;
			bytes += 4;
		}
	}

	return bytes;
}

bool Protocol::parseDegradedSetHeader( size_t offset, uint32_t &listId, uint32_t &chunkId, uint32_t *&original, uint32_t *&remapped, uint32_t &remappedCount, uint8_t &keySize, char *&key, uint32_t &valueSize, char *&value, uint32_t &splitOffset, char *buf, size_t size ) {
	if ( size - offset < PROTO_DEGRADED_SET_SIZE )
		return false;

	char *ptr = buf + offset;
	unsigned char *tmp;
	listId        = ntohl( *( ( uint32_t * )( ptr      ) ) );
	chunkId       = ntohl( *( ( uint32_t * )( ptr +  4 ) ) );
	remappedCount = ntohl( *( ( uint32_t * )( ptr +  8 ) ) );
	keySize = *( ptr + 12 );
	ptr += 13;

	valueSize = 0;
	tmp = ( unsigned char * ) &valueSize;
	tmp[ 1 ] = ptr[ 0 ];
	tmp[ 2 ] = ptr[ 1 ];
	tmp[ 3 ] = ptr[ 2 ];
	valueSize = ntohl( valueSize );
	ptr += 3;

	key = ptr;
	ptr += keySize;

	uint32_t numOfSplit, splitSize;
	if ( LargeObjectUtil::isLarge( keySize, valueSize, &numOfSplit, &splitSize ) ) {
		splitOffset = 0;
		tmp = ( unsigned char * ) &splitOffset;
		tmp[ 1 ] = ptr[ 0 ];
		tmp[ 2 ] = ptr[ 1 ];
		tmp[ 3 ] = ptr[ 2 ];
		splitOffset = ntohl( splitOffset );
		ptr += PROTO_SPLIT_OFFSET_SIZE;

		if ( splitOffset + splitSize > valueSize )
			splitSize = valueSize - splitOffset;

		if ( size - offset < PROTO_DEGRADED_SET_SIZE + PROTO_SPLIT_OFFSET_SIZE + keySize + splitSize + remappedCount * 4 * 4 )
			return false;

		value = ptr;

		ptr += splitSize;
	} else {
		splitOffset = 0;

		if ( size - offset < PROTO_DEGRADED_SET_SIZE + keySize + valueSize + remappedCount * 4 * 4 )
			return false;

		value = ptr;
		ptr += valueSize;
	}

	if ( remappedCount ) {
		uint32_t count = remappedCount * 2;
		original = ( uint32_t * ) ptr;
		remapped = ( ( uint32_t * ) ptr ) + count;
		for ( uint32_t i = 0; i < count; i++ ) {
			original[ i ] = ntohl( original[ i ] );
			remapped[ i ] = ntohl( remapped[ i ] );
		}
	} else {
		original = 0;
		remapped = 0;
	}

	return true;
}

bool Protocol::parseDegradedSetHeader( struct DegradedSetHeader &header, char *buf, size_t size, size_t offset, struct sockaddr_in *target ) {
	if ( ! buf || ! size ) {
		buf = this->buffer.recv;
		size = this->buffer.size;
	}
	return this->parseDegradedSetHeader(
		offset,
		header.listId,
		header.chunkId,
		header.original,
		header.remapped,
		header.remappedCount,
		header.keySize,
		header.key,
		header.valueSize,
		header.value,
		header.splitOffset,
		buf, size
	);
}

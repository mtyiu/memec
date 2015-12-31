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

	return bytes;
}

bool Protocol::parseRemappingLockHeader( size_t offset, uint32_t &remappedCount, uint32_t *&original, uint32_t *&remapped, uint8_t &keySize, char *&key, char *buf, size_t size ) {
	if ( size - offset < PROTO_REMAPPING_LOCK_SIZE )
		return false;

	char *ptr = buf + offset;
	uint32_t count;

	remappedCount = ntohl( *( ( uint32_t * )( ptr      ) ) );
	keySize = ptr[ 4 ];

	if ( size - offset < ( size_t ) PROTO_REMAPPING_LOCK_SIZE + keySize + remappedCount * 4 * 4 )
		return false;

	key = ptr + 5;
	ptr += PROTO_REMAPPING_LOCK_SIZE + keySize;

	count = remappedCount * 2;
	original = ( uint32_t * ) ptr;
	remapped = ( ( uint32_t * ) ptr ) + count;
	for ( uint32_t i = 0; i < count; i++ ) {
		original[ i ] = ntohl( original[ i ] );
		remapped[ i ] = ntohl( remapped[ i ] );
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
		header.remappedCount,
		header.original,
		header.remapped,
		header.keySize,
		header.key,
		buf, size
	);
}

size_t Protocol::generateRemappingSetHeader( uint8_t magic, uint8_t to, uint8_t opcode, uint16_t instanceId, uint32_t requestId, uint32_t listId, uint32_t chunkId, uint8_t keySize, char *key, uint32_t valueSize, char *value, char *sendBuf, uint32_t sockfd, bool isParity, struct sockaddr_in *target ) {
	if ( ! sendBuf ) sendBuf = this->buffer.send;
	uint32_t payload = PROTO_REMAPPING_SET_SIZE + keySize + valueSize;
	if ( target ) payload += 6; // ip + port
	char *buf = sendBuf + PROTO_HEADER_SIZE;
	size_t bytes = this->generateHeader( magic, to, opcode, payload, instanceId, requestId, sendBuf );

	*( ( uint32_t * )( buf     ) ) = htonl( listId );
	*( ( uint32_t * )( buf + 4 ) ) = htonl( chunkId );
	*( ( uint32_t * )( buf + 8 ) ) = htonl( sockfd );
	buf[ 12 ] = isParity;
	bytes += 13;
	buf += 13;

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

	memmove( buf, value, valueSize );
	bytes += valueSize;
	buf += valueSize;

	if ( target ) {
		*( ( uint32_t * )( buf     ) ) = target->sin_addr.s_addr;
		*( ( uint16_t * )( buf + 4 ) ) = target->sin_port;
		bytes += 6;
	}

	return bytes;
}

bool Protocol::parseRemappingSetHeader( size_t offset, uint32_t &listId, uint32_t &chunkId, uint8_t &keySize, char *&key, uint32_t &valueSize, char *&value, char *buf, size_t size, uint32_t &sockfd, bool &isParity, struct sockaddr_in *target ) {
	if ( size - offset < PROTO_REMAPPING_SET_SIZE )
		return false;

	char *ptr = buf + offset;
	unsigned char *tmp;
	listId  = ntohl( *( ( uint32_t * )( ptr      ) ) );
	chunkId = ntohl( *( ( uint32_t * )( ptr +  4 ) ) );
	sockfd = ntohl( *( ( uint32_t * )( ptr +  8 ) ) );
	isParity = *( ptr + 12 );
	keySize = *( ptr + 13 );
	ptr += 14;

	valueSize = 0;
	tmp = ( unsigned char * ) &valueSize;
	tmp[ 1 ] = ptr[ 0 ];
	tmp[ 2 ] = ptr[ 1 ];
	tmp[ 3 ] = ptr[ 2 ];
	valueSize = ntohl( valueSize );
	ptr += 3;

	if ( size - offset < PROTO_REMAPPING_SET_SIZE + keySize + valueSize )
		return false;

	key = ptr;
	value = ptr + keySize;

	ptr += keySize + valueSize;

	if ( size - offset >= PROTO_REMAPPING_SET_SIZE + keySize + valueSize + 6 && target != 0 ) {
		target->sin_addr.s_addr = *( uint32_t * )( ptr );
		target->sin_port = *( uint16_t * )( ptr + 4 );
	}
	return true;
}

bool Protocol::parseRemappingSetHeader( struct RemappingSetHeader &header, char *buf, size_t size, size_t offset, struct sockaddr_in *target ) {
	if ( ! buf || ! size ) {
		buf = this->buffer.recv;
		size = this->buffer.size;
	}
	return this->parseRemappingSetHeader(
		offset,
		header.listId,
		header.chunkId,
		header.keySize,
		header.key,
		header.valueSize,
		header.value,
		buf, size,
		header.sockfd,
		header.remapped,
		target
	);
}

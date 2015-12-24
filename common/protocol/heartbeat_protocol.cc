#include "protocol.hh"

size_t Protocol::generateHeartbeatMessage(
	uint8_t magic, uint8_t to, uint8_t opcode, uint16_t instanceId, uint32_t requestId, uint32_t timestamp,
	LOCK_T *sealedLock, std::unordered_set<Metadata> &sealed, uint32_t &sealedCount,
	LOCK_T *opsLock, std::unordered_map<Key, OpMetadata> &ops, uint32_t &opsCount,
	bool &isCompleted
) {
	char *buf = this->buffer.send + PROTO_HEADER_SIZE;
	size_t bytes = PROTO_HEADER_SIZE;
	uint32_t *sealedPtr, *opsPtr;
	uint8_t *isLastPtr;
	std::unordered_set<Metadata>::iterator sealedIt;
	std::unordered_map<Key, OpMetadata>::iterator opsIt;

	sealedCount = 0;
	opsCount = 0;
	isCompleted = true;

	*( ( uint32_t * )( buf ) ) = htonl( timestamp );
	sealedPtr       = ( uint32_t * )( buf +  4 );
	opsPtr          = ( uint32_t * )( buf +  8 );
	isLastPtr       = ( uint8_t *  )( buf + 12 );

	buf += PROTO_HEARTBEAT_SIZE;
	bytes += PROTO_HEARTBEAT_SIZE;

	/**** Sealed chunks *****/
	LOCK( sealedLock );
	for ( sealedIt = sealed.begin(); sealedIt != sealed.end(); sealedIt++ ) {
		const Metadata &metadata = *sealedIt;
		if ( this->buffer.size >= bytes + PROTO_METADATA_SIZE ) {
			*( ( uint32_t * )( buf     ) ) = htonl( metadata.listId );
			*( ( uint32_t * )( buf + 4 ) ) = htonl( metadata.stripeId );
			*( ( uint32_t * )( buf + 8 ) ) = htonl( metadata.chunkId );
			buf   += PROTO_METADATA_SIZE;
			bytes += PROTO_METADATA_SIZE;
			sealedCount++;
		} else {
			isCompleted = false;
			break;
		}
	}
	sealed.erase( sealed.begin(), sealedIt );
	UNLOCK( sealedLock );

	/***** Keys in SET and DELETE requests *****/
	LOCK( opsLock );
	for ( opsIt = ops.begin(); opsIt != ops.end(); opsIt++ ) {
		Key key = opsIt->first;
		const OpMetadata &opMetadata = opsIt->second;
		if ( this->buffer.size >= bytes + PROTO_KEY_OP_METADATA_SIZE + key.size ) {
			buf[ 0 ] = key.size;
			buf[ 1 ] = opMetadata.opcode;
			*( ( uint32_t * )( buf + 2 ) ) = htonl( opMetadata.listId );
			*( ( uint32_t * )( buf + 6 ) ) = htonl( opMetadata.stripeId );
			*( ( uint32_t * )( buf + 10 ) ) = htonl( opMetadata.chunkId );

			buf += PROTO_KEY_OP_METADATA_SIZE;
			memcpy( buf, key.data, key.size );
			buf += key.size;
			bytes += PROTO_KEY_OP_METADATA_SIZE + key.size;
			opsCount++;
			key.free();
		} else {
			isCompleted = false;
			break;
		}
	}
	ops.erase( ops.begin(), opsIt );
	UNLOCK( opsLock );

	*sealedPtr = htonl( sealedCount );
	*opsPtr = htonl( opsCount );
	*isLastPtr = isCompleted? 1 : 0;

	// TODO tell coordinator whether this is the last packet of records

	this->generateHeader( magic, to, opcode, bytes - PROTO_HEADER_SIZE, instanceId, requestId );

	return bytes;
}

size_t Protocol::generateHeartbeatMessage( uint8_t magic, uint8_t to, uint8_t opcode, uint16_t instanceId, uint32_t requestId, uint32_t timestamp, uint32_t sealed, uint32_t keys, bool isLast ) {
	char *buf = this->buffer.send + PROTO_HEADER_SIZE;
	size_t bytes = this->generateHeader( magic, to, opcode, PROTO_HEARTBEAT_SIZE, instanceId, requestId );

	*( ( uint32_t * )( buf      ) ) = htonl( timestamp );
	*( ( uint32_t * )( buf +  4 ) ) = htonl( sealed );
	*( ( uint32_t * )( buf +  8 ) ) = htonl( keys );
	buf[ 12 ] = isLast;

	bytes += PROTO_HEARTBEAT_SIZE;

	return bytes;
}

bool Protocol::parseHeartbeatHeader( size_t offset, uint32_t &timestamp, uint32_t &sealed, uint32_t &keys, bool isLast, char *buf, size_t size ) {
	if ( size - offset < PROTO_HEARTBEAT_SIZE )
		return false;

	char *ptr = buf + offset;
	timestamp = ntohl( *( ( uint32_t * )( ptr      ) ) );
	sealed    = ntohl( *( ( uint32_t * )( ptr +  4 ) ) );
	keys      = ntohl( *( ( uint32_t * )( ptr +  8 ) ) );
	isLast    = ( *( ( uint8_t * )( ptr + 12 ) ) == 0 ) ? false : true;

	return true;
}

bool Protocol::parseMetadataHeader( size_t offset, uint32_t &listId, uint32_t &stripeId, uint32_t &chunkId, char *buf, size_t size ) {
	if ( size - offset < PROTO_METADATA_SIZE )
		return false;

	char *ptr = buf + offset;
	listId   = ntohl( *( ( uint32_t * )( ptr     ) ) );
	stripeId = ntohl( *( ( uint32_t * )( ptr + 4 ) ) );
	chunkId  = ntohl( *( ( uint32_t * )( ptr + 8 ) ) );

	return true;
}

bool Protocol::parseKeyOpMetadataHeader( size_t offset, uint8_t &keySize, uint8_t &opcode, uint32_t &listId, uint32_t &stripeId, uint32_t &chunkId, char *&key, char *buf, size_t size ) {
	if ( size - offset < PROTO_KEY_OP_METADATA_SIZE )
		return false;

	char *ptr = buf + offset;
	keySize  = ( uint8_t ) ptr[ 0 ];
	opcode   = ( uint8_t ) ptr[ 1 ];
	listId   = ntohl( *( ( uint32_t * )( ptr +  2 ) ) );
	stripeId = ntohl( *( ( uint32_t * )( ptr +  6 ) ) );
	chunkId  = ntohl( *( ( uint32_t * )( ptr + 10 ) ) );

	if ( size - offset < PROTO_KEY_OP_METADATA_SIZE + ( size_t ) keySize )
		return false;

	key = ptr + PROTO_KEY_OP_METADATA_SIZE;

	return true;
}

bool Protocol::parseHeartbeatHeader( struct HeartbeatHeader &header, char *buf, size_t size, size_t offset ) {
	if ( ! buf || ! size ) {
		buf = this->buffer.recv;
		size = this->buffer.size;
	}
	return this->parseHeartbeatHeader(
		offset,
		header.timestamp,
		header.sealed,
		header.keys,
		header.isLast,
		buf, size
	);
}

bool Protocol::parseMetadataHeader( struct MetadataHeader &header, size_t &bytes, char *buf, size_t size, size_t offset ) {
	if ( ! buf || ! size ) {
		buf = this->buffer.recv;
		size = this->buffer.size;
	}
	bool ret = this->parseMetadataHeader(
		offset,
		header.listId,
		header.stripeId,
		header.chunkId,
		buf, size
	);
	bytes = PROTO_METADATA_SIZE;
	return ret;
}

bool Protocol::parseKeyOpMetadataHeader( struct KeyOpMetadataHeader &header, size_t &bytes, char *buf, size_t size, size_t offset ) {
	if ( ! buf || ! size ) {
		buf = this->buffer.recv;
		size = this->buffer.size;
	}
	bool ret = this->parseKeyOpMetadataHeader(
		offset,
		header.keySize,
		header.opcode,
		header.listId,
		header.stripeId,
		header.chunkId,
		header.key,
		buf, size
	);
	bytes = PROTO_KEY_OP_METADATA_SIZE + header.keySize;
	return ret;
}

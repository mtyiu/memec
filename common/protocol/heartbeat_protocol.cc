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
			bytes += ProtocolUtil::write4Bytes( buf, metadata.listId   );
			bytes += ProtocolUtil::write4Bytes( buf, metadata.stripeId );
			bytes += ProtocolUtil::write4Bytes( buf, metadata.chunkId  );
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
			bytes += ProtocolUtil::write1Byte ( buf, key.size          );
			bytes += ProtocolUtil::write1Byte ( buf, opMetadata.opcode );
			bytes += ProtocolUtil::write4Bytes( buf, opMetadata.listId );
			bytes += ProtocolUtil::write4Bytes( buf, opMetadata.stripeId );
			bytes += ProtocolUtil::write4Bytes( buf, opMetadata.chunkId );
			bytes += ProtocolUtil::write4Bytes( buf, opMetadata.timestamp );
			bytes += ProtocolUtil::write( buf, key.data, key.size );
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
	*isLastPtr = isCompleted ? 1 : 0;
	this->generateHeader( magic, to, opcode, bytes - PROTO_HEADER_SIZE, instanceId, requestId );
	return bytes;
}

size_t Protocol::generateHeartbeatMessage( uint8_t magic, uint8_t to, uint8_t opcode, uint16_t instanceId, uint32_t requestId, uint32_t timestamp, uint32_t sealed, uint32_t keys, bool isLast ) {
	char *buf = this->buffer.send + PROTO_HEADER_SIZE;
	size_t bytes = this->generateHeader( magic, to, opcode, PROTO_HEARTBEAT_SIZE, instanceId, requestId );
	bytes += ProtocolUtil::write4Bytes( buf, timestamp );
	bytes += ProtocolUtil::write4Bytes( buf, sealed    );
	bytes += ProtocolUtil::write4Bytes( buf, keys      );
	bytes += ProtocolUtil::write1Byte ( buf, isLast    );
	return bytes;
}

bool Protocol::parseHeartbeatHeader( struct HeartbeatHeader &header, char *buf, size_t size, size_t offset ) {
	if ( ! buf || ! size ) {
		buf = this->buffer.recv;
		size = this->buffer.size;
	}
	if ( size - offset < PROTO_HEARTBEAT_SIZE ) return false;
	char *ptr = buf + offset;
	header.timestamp = ProtocolUtil::read4Bytes( ptr );
	header.sealed    = ProtocolUtil::read4Bytes( ptr );
	header.keys      = ProtocolUtil::read4Bytes( ptr );
	header.isLast    = ProtocolUtil::read1Byte ( ptr );
	return true;
}

bool Protocol::parseMetadataHeader( struct MetadataHeader &header, size_t &bytes, char *buf, size_t size, size_t offset ) {
	if ( ! buf || ! size ) {
		buf = this->buffer.recv;
		size = this->buffer.size;
	}
	if ( size - offset < PROTO_METADATA_SIZE ) return false;
	char *ptr = buf + offset;
	header.listId   = ProtocolUtil::read4Bytes( ptr );
	header.stripeId = ProtocolUtil::read4Bytes( ptr );
	header.chunkId  = ProtocolUtil::read4Bytes( ptr );
	bytes = PROTO_METADATA_SIZE;
	return true;
}

bool Protocol::parseKeyOpMetadataHeader( struct KeyOpMetadataHeader &header, size_t &bytes, char *buf, size_t size, size_t offset ) {
	if ( ! buf || ! size ) {
		buf = this->buffer.recv;
		size = this->buffer.size;
	}
	if ( size - offset < PROTO_KEY_OP_METADATA_SIZE ) return false;
	char *ptr = buf + offset;
	header.keySize   = ProtocolUtil::read1Byte ( ptr );
	header.opcode    = ProtocolUtil::read1Byte ( ptr );
	header.listId    = ProtocolUtil::read4Bytes( ptr );
	header.stripeId  = ProtocolUtil::read4Bytes( ptr );
	header.chunkId   = ProtocolUtil::read4Bytes( ptr );
	header.timestamp = ProtocolUtil::read4Bytes( ptr );
	header.key = ptr;
	bytes = PROTO_KEY_OP_METADATA_SIZE + ( size_t ) header.keySize;
	return ( size - offset >= PROTO_KEY_OP_METADATA_SIZE + ( size_t ) header.keySize );
}

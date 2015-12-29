#include "protocol.hh"

size_t Protocol::generateMetadataBackupMessage(
	uint8_t magic, uint8_t to, uint8_t opcode,
	uint16_t instanceId, uint32_t requestId,
	uint32_t addr, uint16_t port,
	LOCK_T *lock,
	std::unordered_multimap<uint32_t, Metadata> &sealed, uint32_t &sealedCount,
	std::unordered_map<Key, MetadataBackup> &ops, uint32_t &opsCount,
	bool &isCompleted
) {
	char *buf = this->buffer.send + PROTO_HEADER_SIZE;
	size_t bytes = PROTO_HEADER_SIZE;
	uint32_t maxTimestamp = 0;
	uint32_t *timestampPtr, *sealedPtr, *opsPtr;
	uint8_t *isLastPtr;
	std::unordered_multimap<uint32_t, Metadata>::iterator sealedIt;
	std::unordered_map<Key, MetadataBackup>::iterator opsIt;

	sealedCount = 0;
	opsCount = 0;
	isCompleted = true;

	// Already in network-byte order
	*( ( uint32_t * )( buf     ) ) = addr;
	*( ( uint16_t * )( buf + 4 ) ) = port;

	buf += PROTO_ADDRESS_SIZE;
	bytes += PROTO_ADDRESS_SIZE;

	timestampPtr    = ( uint32_t * )( buf      );
	sealedPtr       = ( uint32_t * )( buf +  4 );
	opsPtr          = ( uint32_t * )( buf +  8 );
	isLastPtr       = ( uint8_t *  )( buf + 12 );

	buf += PROTO_HEARTBEAT_SIZE;
	bytes += PROTO_HEARTBEAT_SIZE;

	/**** Sealed chunks *****/
	LOCK( lock );
	for ( sealedIt = sealed.begin(); sealedIt != sealed.end(); sealedIt++ ) {
		const Metadata &metadata = sealedIt->second;

		if ( maxTimestamp < sealedIt->first )
			maxTimestamp = sealedIt->first;

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

	/***** Keys in SET and DELETE requests *****/
	for ( opsIt = ops.begin(); opsIt != ops.end(); opsIt++ ) {
		Key key = opsIt->first;
		const MetadataBackup &metadataBackup = opsIt->second;

		if ( maxTimestamp < metadataBackup.timestamp )
			maxTimestamp = metadataBackup.timestamp;

		if ( this->buffer.size >= bytes + PROTO_KEY_OP_METADATA_SIZE + key.size ) {
			buf[ 0 ] = key.size;
			buf[ 1 ] = metadataBackup.opcode;
			*( ( uint32_t * )( buf + 2 ) ) = htonl( metadataBackup.listId );
			*( ( uint32_t * )( buf + 6 ) ) = htonl( metadataBackup.stripeId );
			*( ( uint32_t * )( buf + 10 ) ) = htonl( metadataBackup.chunkId );
			*( ( uint32_t * )( buf + 14 ) ) = htonl( metadataBackup.timestamp );

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
	UNLOCK( lock );

	*timestampPtr = htonl( maxTimestamp );
	*sealedPtr = htonl( sealedCount );
	*opsPtr = htonl( opsCount );
	*isLastPtr = isCompleted ? 1 : 0;

	// printf( "isCompleted = %d; count = %lu\n", isCompleted, ops.size() );

	this->generateHeader( magic, to, opcode, bytes - PROTO_HEADER_SIZE, instanceId, requestId );

	return bytes;
}

bool Protocol::parseMetadataBackupMessage( struct AddressHeader &address, struct HeartbeatHeader &heartbeat, char *buf, size_t size, size_t offset ) {
	if ( ! buf || ! size ) {
		buf = this->buffer.recv;
		size = this->buffer.size;
	}
	return this->parseAddressHeader(
		offset,
		address.addr,
		address.port,
		buf, size
	) && this->parseHeartbeatHeader(
		offset + PROTO_ADDRESS_SIZE,
		heartbeat.timestamp,
		heartbeat.sealed,
		heartbeat.keys,
		heartbeat.isLast,
		buf, size
	);
}

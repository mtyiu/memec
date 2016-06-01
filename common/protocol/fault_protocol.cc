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
	bytes += ProtocolUtil::write4Bytes( buf, addr, false );
	bytes += ProtocolUtil::write2Bytes( buf, port, false );

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

	/***** Keys in SET and DELETE requests *****/
	for ( opsIt = ops.begin(); opsIt != ops.end(); opsIt++ ) {
		Key key = opsIt->first;
		const MetadataBackup &metadataBackup = opsIt->second;
		if ( maxTimestamp < metadataBackup.timestamp )
			maxTimestamp = metadataBackup.timestamp;
		if ( this->buffer.size >= bytes + PROTO_KEY_OP_METADATA_SIZE + key.size ) {
			bytes += ProtocolUtil::write1Byte ( buf, key.size                 );
			bytes += ProtocolUtil::write1Byte ( buf, metadataBackup.opcode    );
			bytes += ProtocolUtil::write4Bytes( buf, metadataBackup.listId    );
			bytes += ProtocolUtil::write4Bytes( buf, metadataBackup.stripeId  );
			bytes += ProtocolUtil::write4Bytes( buf, metadataBackup.chunkId   );
			bytes += ProtocolUtil::write4Bytes( buf, metadataBackup.timestamp );
			bytes += ProtocolUtil::write( buf, key.data, key.size );
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
	this->generateHeader( magic, to, opcode, bytes - PROTO_HEADER_SIZE, instanceId, requestId );
	return bytes;
}

bool Protocol::parseMetadataBackupMessage( struct AddressHeader &address, struct HeartbeatHeader &heartbeat, char *buf, size_t size, size_t offset ) {
	if ( ! buf || ! size ) {
		buf = this->buffer.recv;
		size = this->buffer.size;
	}
	return this->parseAddressHeader( address, buf, size, offset ) && this->parseHeartbeatHeader( heartbeat, buf, size, offset + PROTO_ADDRESS_SIZE );
}

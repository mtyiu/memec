#include "protocol.hh"

char *MasterProtocol::reqRemappingSetLock( size_t &size, uint32_t id, uint32_t listId, std::vector<uint32_t> chunkId, uint32_t reqRemapState , char *key, uint8_t keySize, uint32_t sockfd ) {
	// -- common/protocol/remap_protocol.cc --
	size = this->generateRemappingLockHeader(
		PROTO_MAGIC_REQUEST,
		PROTO_MAGIC_TO_COORDINATOR,
		PROTO_OPCODE_REMAPPING_LOCK,
		id,
		listId,
		chunkId[ 0 ],
		reqRemapState,
		keySize,
		key,
		sockfd,
		( chunkId.size() - 1 ) * 4 + 1
	);

	// append the list of parity slave ( may be remapped )
	*( this->buffer.send + size ) = ( uint8_t ) chunkId.size() - 1;
	size += 1;
	for ( uint32_t i = 1; i < chunkId.size(); i++, size += 4 ) {
		*( ( uint32_t * )( this->buffer.send + size ) ) = htonl( chunkId[ i ] );
	}

	return this->buffer.send;
}

char *MasterProtocol::reqRemappingSet( size_t &size, uint32_t id, uint32_t listId, uint32_t chunkId, char *key, uint8_t keySize, char *value, uint32_t valueSize, char *buf, uint32_t sockfd, bool isParity, struct sockaddr_in *target ) {
	// -- common/protocol/remap_protocol.cc --
	if ( ! buf ) buf = this->buffer.send;
	size = this->generateRemappingSetHeader(
		PROTO_MAGIC_REQUEST,
		PROTO_MAGIC_TO_SLAVE,
		PROTO_OPCODE_REMAPPING_SET,
		id,
		listId,
		chunkId,
		keySize,
		key,
		valueSize,
		value,
		buf,
		sockfd,
		isParity,
		target
	);
	return buf;
}

char *MasterProtocol::resSyncRemappingRecords( size_t &size, uint32_t id ) {
	// -- common/protocol/protocol.cc --
	size = this->generateHeader(
		PROTO_MAGIC_REMAPPING,
		PROTO_MAGIC_TO_COORDINATOR,
		PROTO_OPCODE_SYNC,
		0,
		id
	);
	return this->buffer.send;
}

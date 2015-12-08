#include "protocol.hh"

bool CoordinatorProtocol::parseRemappingLockHeader( struct RemappingLockHeader &header, char *buf, size_t size, std::vector<uint32_t> *remapList, size_t offset ) {
	bool ret = Protocol::parseRemappingLockHeader( header, buf, size, offset );
	char *payload = buf + offset + PROTO_REMAPPING_LOCK_SIZE + header.keySize;
	uint32_t listCount = payload[ 0 ];
	payload += 1;
	for ( uint32_t i = 0; i < listCount; i++, payload += 4 ) {
		remapList->push_back( ntohl( *( ( uint32_t * )( payload ) ) ) );
	}
	return ret;
}

char *CoordinatorProtocol::resRemappingSetLock( size_t &size, uint32_t id, bool success, uint32_t listId, uint32_t chunkId, bool isRemapped, uint8_t keySize, char *key, uint32_t sockfd ) {
	// -- common/protocol/remap_protocol.cc --
	size = this->generateRemappingLockHeader(
		success ? PROTO_MAGIC_RESPONSE_SUCCESS : PROTO_MAGIC_RESPONSE_FAILURE,
		PROTO_MAGIC_TO_MASTER,
		PROTO_OPCODE_REMAPPING_LOCK,
		id,
		listId,
		chunkId,
		isRemapped,
		keySize,
		key,
		sockfd
	);
	return this->buffer.send;
}

char *CoordinatorProtocol::forwardRemappingRecords( size_t &size, uint32_t id, char* message ) {
	// -- common/protocol/protocol.cc --
	size_t headerSize = this->generateHeader(
		PROTO_MAGIC_REMAPPING,
		PROTO_MAGIC_TO_MASTER,
		PROTO_OPCODE_SYNC,
		size,
		id
	);
	memcpy( this->buffer.send + headerSize, message, size );
	size += headerSize;
	return this->buffer.send;
}

char *CoordinatorProtocol::reqSyncRemappingRecord( size_t &size, uint32_t id, std::unordered_map<Key, RemappingRecord> &remappingRecords, LOCK_T* lock, bool &isLast, char* buffer ) {
	// -- common/protocol/remap_protocol.cc --
	size_t remapCount = 0;
	if ( ! buffer ) buffer = this->buffer.send;
	size = this->generateRemappingRecordMessage(
		PROTO_MAGIC_REMAPPING,
		PROTO_MAGIC_TO_MASTER,
		PROTO_OPCODE_SYNC,
		id,
		lock,
		remappingRecords,
		remapCount,
		buffer
	);
	isLast = ( remapCount == 0 );

	return buffer;
}

char *CoordinatorProtocol::reqSyncRemappedParity( size_t &size, uint32_t id, struct sockaddr_in target, char* buffer ) {
	// -- common/protocol/address_protocol.cc --
	if ( ! buffer ) buffer = this->buffer.send;
	size = this->generateAddressHeader(
		PROTO_MAGIC_REMAPPING,
		PROTO_MAGIC_TO_SLAVE,
		PROTO_OPCODE_PARITY_MIGRATE,
		id,
		target.sin_addr.s_addr,
		target.sin_port,
		buffer
	);
	return buffer;
}

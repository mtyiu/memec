#include "protocol.hh"

char *SlaveProtocol::resRemappingSet( size_t &size, bool toMaster, uint16_t instanceId, uint32_t requestId, bool success, uint32_t listId, uint32_t chunkId, uint8_t keySize, char *key, uint32_t sockfd, bool remapped ) {
	// -- common/protocol/remap_protocol.cc --
	size = this->generateRemappingLockHeader(
		success ? PROTO_MAGIC_RESPONSE_SUCCESS : PROTO_MAGIC_RESPONSE_FAILURE,
		toMaster ? PROTO_MAGIC_TO_MASTER : PROTO_MAGIC_TO_SLAVE,
		PROTO_OPCODE_REMAPPING_SET,
		instanceId, requestId,
		listId,
		chunkId,
		remapped, // TODO whether this is a true remapped key
		keySize,
		key,
		sockfd
	);
	return this->buffer.send;
}

char *SlaveProtocol::resRemapParity( size_t &size, uint16_t instanceId, uint32_t requestId ) {
	// -- common/protocol/protocol.cc --
	size = this->generateHeader(
		PROTO_MAGIC_RESPONSE_SUCCESS,
		PROTO_MAGIC_TO_COORDINATOR,
		PROTO_OPCODE_PARITY_MIGRATE,
		0,
		instanceId, requestId
	);
	return this->buffer.send;
};

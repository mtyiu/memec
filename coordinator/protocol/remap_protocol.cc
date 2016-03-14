#include "protocol.hh"

char *CoordinatorProtocol::resRemappingSetLock( size_t &size, uint16_t instanceId, uint32_t requestId, bool success, uint32_t *original, uint32_t *remapped, uint32_t remappedCount, uint8_t keySize, char *key ) {
	// -- common/protocol/remap_protocol.cc --
	size = this->generateRemappingLockHeader(
		success ? PROTO_MAGIC_RESPONSE_SUCCESS : PROTO_MAGIC_RESPONSE_FAILURE,
		PROTO_MAGIC_TO_CLIENT,
		PROTO_OPCODE_REMAPPING_LOCK,
		instanceId, requestId,
		original, remapped, remappedCount,
		keySize, key
	);
	return this->buffer.send;
}

char *CoordinatorProtocol::reqSyncRemappedData( size_t &size, uint16_t instanceId, uint32_t requestId, struct sockaddr_in target, char* buffer ) {
	// -- common/protocol/address_protocol.cc --
	if ( ! buffer ) buffer = this->buffer.send;
	size = this->generateAddressHeader(
		PROTO_MAGIC_REMAPPING,
		PROTO_MAGIC_TO_SERVER,
		PROTO_OPCODE_PARITY_MIGRATE,
		instanceId, requestId,
		target.sin_addr.s_addr,
		target.sin_port,
		buffer
	);
	return buffer;
}

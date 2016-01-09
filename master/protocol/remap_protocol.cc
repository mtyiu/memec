#include "protocol.hh"

char *MasterProtocol::reqRemappingSetLock( size_t &size, uint16_t instanceId, uint32_t requestId, uint32_t *original, uint32_t *remapped, uint32_t remappedCount, char *key, uint8_t keySize ) {
	// -- common/protocol/remap_protocol.cc --
	size = this->generateRemappingLockHeader(
		PROTO_MAGIC_REQUEST,
		PROTO_MAGIC_TO_COORDINATOR,
		PROTO_OPCODE_REMAPPING_LOCK,
		instanceId, requestId,
		original, remapped, remappedCount,
		keySize, key
	);
	return this->buffer.send;
}

char *MasterProtocol::reqRemappingSet( size_t &size, uint16_t instanceId, uint32_t requestId, uint32_t listId, uint32_t chunkId, uint32_t *original, uint32_t *remapped, uint32_t remappedCount, char *key, uint8_t keySize, char *value, uint32_t valueSize, char *buf ) {
	// -- common/protocol/remap_protocol.cc --
	if ( ! buf ) buf = this->buffer.send;
	size = this->generateRemappingSetHeader(
		PROTO_MAGIC_REQUEST,
		PROTO_MAGIC_TO_SLAVE,
		PROTO_OPCODE_REMAPPING_SET,
		instanceId, requestId,
		listId, chunkId,
		original, remapped, remappedCount,
		keySize, key,
		valueSize, value,
		buf
	);
	return buf;
}

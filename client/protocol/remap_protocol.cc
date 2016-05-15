#include "protocol.hh"

char *ClientProtocol::reqDegradedSetLock( size_t &size, uint16_t instanceId, uint32_t requestId, uint32_t *original, uint32_t *remapped, uint32_t remappedCount, char *key, uint8_t keySize, bool isLarge ) {
	// -- common/protocol/remap_protocol.cc --
	size = this->generateRemappingLockHeader(
		PROTO_MAGIC_REQUEST,
		PROTO_MAGIC_TO_COORDINATOR,
		PROTO_OPCODE_REMAPPING_LOCK,
		instanceId, requestId,
		original, remapped, remappedCount,
		keySize, key, isLarge
	);
	return this->buffer.send;
}

char *ClientProtocol::reqDegradedSet( size_t &size, uint16_t instanceId, uint32_t requestId, uint32_t listId, uint32_t chunkId, uint32_t *original, uint32_t *remapped, uint32_t remappedCount, char *key, uint8_t keySize, char *value, uint32_t valueSize, uint32_t splitOffset, uint32_t splitSize, char *buf ) {
	// -- common/protocol/remap_protocol.cc --
	if ( ! buf ) buf = this->buffer.send;
	size = this->generateDegradedSetHeader(
		PROTO_MAGIC_REQUEST,
		PROTO_MAGIC_TO_SERVER,
		PROTO_OPCODE_DEGRADED_SET,
		instanceId, requestId,
		listId, chunkId,
		original, remapped, remappedCount,
		keySize, key,
		valueSize, value,
		splitOffset, splitSize,
		buf
	);
	return buf;
}

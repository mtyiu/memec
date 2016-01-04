#include "protocol.hh"

char *SlaveProtocol::reqDegradedSet(
	size_t &size, uint16_t instanceId, uint32_t requestId,
	uint8_t opcode, uint32_t listId, uint32_t chunkId,
	uint8_t keySize, char *key,
	uint32_t valueSize, char *value,
	uint32_t valueUpdateSize, uint32_t valueUpdateOffset, char *valueUpdate
) {
	// -- common/protocol/degraded_protocol.cc --
	size = this->generateDegradedSetReqHeader(
		PROTO_MAGIC_REQUEST,
		PROTO_MAGIC_TO_SLAVE,
		PROTO_OPCODE_DEGRADED_SET,
		instanceId, requestId,
		opcode, listId, chunkId,
		keySize, key,
		valueSize, value,
		valueUpdateSize, valueUpdateOffset, valueUpdate
	);
	return this->buffer.send;
}

char *SlaveProtocol::resDegradedSet(
	size_t &size, uint16_t instanceId, uint32_t requestId, bool success,
	uint8_t opcode, uint32_t listId, uint32_t chunkId,
	uint8_t keySize, char *key,
	uint32_t valueSize,
	uint32_t valueUpdateSize, uint32_t valueUpdateOffset
) {
	// -- common/protocol/degraded_protocol.cc --
	size = this->generateDegradedSetResHeader(
		success ? PROTO_MAGIC_RESPONSE_SUCCESS : PROTO_MAGIC_RESPONSE_FAILURE,
		PROTO_MAGIC_TO_SLAVE,
		PROTO_OPCODE_DEGRADED_SET,
		instanceId, requestId,
		opcode, listId, chunkId,
		keySize, key,
		valueSize,
		valueUpdateSize, valueUpdateOffset
	);
	return this->buffer.send;
}

char *SlaveProtocol::reqGet( size_t &size, uint16_t instanceId, uint32_t requestId, uint32_t listId, uint32_t chunkId, uint8_t keySize, char *key ) {
	// -- common/protocol/degraded_protocol.cc --
	size = this->generateListStripeKeyHeader(
		PROTO_MAGIC_REQUEST,
		PROTO_MAGIC_TO_SLAVE,
		PROTO_OPCODE_GET,
		instanceId, requestId,
		listId,
		chunkId,
		keySize,
		key
	);
	return this->buffer.send;
}

char *SlaveProtocol::resReleaseDegradedLock( size_t &size, uint16_t instanceId, uint32_t requestId, uint32_t count ) {
	// -- common/protocol/degraded_protocol.cc --
	size = this->generateDegradedReleaseResHeader(
		PROTO_MAGIC_RESPONSE_SUCCESS,
		PROTO_MAGIC_TO_COORDINATOR,
		PROTO_OPCODE_RELEASE_DEGRADED_LOCKS,
		instanceId, requestId,
		count
	);
	return this->buffer.send;
}

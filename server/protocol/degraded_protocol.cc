#include "protocol.hh"

char *SlaveProtocol::reqForwardKey(
	size_t &size, uint16_t instanceId, uint32_t requestId,
	uint8_t opcode, uint32_t listId, uint32_t stripeId, uint32_t chunkId,
	uint8_t keySize, char *key,
	uint32_t valueSize, char *value,
	uint32_t valueUpdateSize, uint32_t valueUpdateOffset, char *valueUpdate
) {
	// -- common/protocol/degraded_protocol.cc --
	size = this->generateForwardKeyReqHeader(
		PROTO_MAGIC_REQUEST,
		PROTO_MAGIC_TO_SERVER,
		PROTO_OPCODE_FORWARD_KEY,
		instanceId, requestId,
		opcode, listId, stripeId, chunkId,
		keySize, key,
		valueSize, value,
		valueUpdateSize, valueUpdateOffset, valueUpdate
	);
	return this->buffer.send;
}

char *SlaveProtocol::resForwardKey(
	size_t &size, uint16_t instanceId, uint32_t requestId, bool success,
	uint8_t opcode, uint32_t listId, uint32_t stripeId, uint32_t chunkId,
	uint8_t keySize, char *key,
	uint32_t valueSize,
	uint32_t valueUpdateSize, uint32_t valueUpdateOffset
) {
	// -- common/protocol/degraded_protocol.cc --
	size = this->generateForwardKeyResHeader(
		success ? PROTO_MAGIC_RESPONSE_SUCCESS : PROTO_MAGIC_RESPONSE_FAILURE,
		PROTO_MAGIC_TO_SERVER,
		PROTO_OPCODE_FORWARD_KEY,
		instanceId, requestId,
		opcode, listId, stripeId, chunkId,
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
		PROTO_MAGIC_TO_SERVER,
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

char *SlaveProtocol::reqForwardChunk(
	size_t &size, uint16_t instanceId, uint32_t requestId,
	uint32_t listId, uint32_t stripeId, uint32_t chunkId,
	uint32_t chunkSize, uint32_t chunkOffset, char *chunkData
) {
	size = this->generateChunkDataHeader(
		PROTO_MAGIC_REQUEST,
		PROTO_MAGIC_TO_SERVER,
		PROTO_OPCODE_FORWARD_CHUNK,
		instanceId, requestId,
		listId, stripeId, chunkId,
		chunkSize, chunkOffset, chunkData,
		0, 0
	);
	return this->buffer.send;
}

char *SlaveProtocol::resForwardChunk(
	size_t &size, uint16_t instanceId, uint32_t requestId, bool success,
	uint32_t listId, uint32_t stripeId, uint32_t chunkId
) {
	size = this->generateChunkHeader(
		success ? PROTO_MAGIC_RESPONSE_SUCCESS : PROTO_MAGIC_RESPONSE_FAILURE,
		PROTO_MAGIC_TO_SERVER,
		PROTO_OPCODE_FORWARD_CHUNK,
		instanceId, requestId,
		listId, stripeId, chunkId
	);
	return this->buffer.send;
}

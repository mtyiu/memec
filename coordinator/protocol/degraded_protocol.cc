#include "protocol.hh"

char *CoordinatorProtocol::resDegradedLock( size_t &size, uint16_t instanceId, uint32_t requestId, bool isLocked, bool isSealed, uint8_t keySize, char *key, uint32_t listId, uint32_t stripeId, uint32_t srcDataChunkId, uint32_t dstDataChunkId, uint32_t srcParityChunkId, uint32_t dstParityChunkId ) {
	// -- common/protocol/degraded_protocol.cc --
	size = this->generateDegradedLockResHeader(
		isLocked ? PROTO_MAGIC_RESPONSE_SUCCESS : PROTO_MAGIC_RESPONSE_FAILURE,
		PROTO_MAGIC_TO_MASTER,
		PROTO_OPCODE_DEGRADED_LOCK,
		instanceId, requestId,
		isLocked,
		isSealed,
		keySize, key,
		listId, stripeId,
		srcDataChunkId, dstDataChunkId,
		srcParityChunkId, dstParityChunkId
	);
	return this->buffer.send;
}

char *CoordinatorProtocol::resDegradedLock( size_t &size, uint16_t instanceId, uint32_t requestId, uint8_t keySize, char *key, uint32_t listId, uint32_t srcDataChunkId, uint32_t dstDataChunkId, uint32_t srcParityChunkId, uint32_t dstParityChunkId ) {
	// -- common/protocol/degraded_protocol.cc --
	size = this->generateDegradedLockResHeader(
		PROTO_MAGIC_RESPONSE_FAILURE,
		PROTO_MAGIC_TO_MASTER,
		PROTO_OPCODE_DEGRADED_LOCK,
		instanceId, requestId,
		keySize, key,
		listId,
		srcDataChunkId, dstDataChunkId,
		srcParityChunkId, dstParityChunkId
	);
	return this->buffer.send;
}

char *CoordinatorProtocol::resDegradedLock( size_t &size, uint16_t instanceId, uint32_t requestId, bool exist, uint8_t keySize, char *key, uint32_t listId, uint32_t srcDataChunkId, uint32_t srcParityChunkId ) {
	// -- common/protocol/degraded_protocol.cc --
	size = this->generateDegradedLockResHeader(
		PROTO_MAGIC_RESPONSE_FAILURE,
		PROTO_MAGIC_TO_MASTER,
		PROTO_OPCODE_DEGRADED_LOCK,
		instanceId, requestId,
		exist,
		keySize, key,
		listId, srcDataChunkId, srcParityChunkId
	);
	return this->buffer.send;
}

char *CoordinatorProtocol::reqReleaseDegradedLock( size_t &size, uint16_t instanceId, uint32_t requestId, std::vector<Metadata> &chunks, bool &isCompleted ) {
	// -- common/protocol/degraded_protocol.cc --
	size = this->generateDegradedReleaseReqHeader(
		PROTO_MAGIC_REQUEST,
		PROTO_MAGIC_TO_SLAVE,
		PROTO_OPCODE_RELEASE_DEGRADED_LOCKS,
		instanceId, requestId,
		chunks,
		isCompleted
	);
	return this->buffer.send;
}

#include "protocol.hh"

char *CoordinatorProtocol::resDegradedLock( size_t &size, uint32_t id, bool isLocked, bool isSealed, uint8_t keySize, char *key, uint32_t listId, uint32_t stripeId, uint32_t srcDataChunkId, uint32_t dstDataChunkId, uint32_t srcParityChunkId, uint32_t dstParityChunkId ) {
	// -- common/protocol/degraded_protocol.cc --
	size = this->generateDegradedLockResHeader(
		isLocked ? PROTO_MAGIC_RESPONSE_SUCCESS : PROTO_MAGIC_RESPONSE_FAILURE,
		PROTO_MAGIC_TO_MASTER,
		PROTO_OPCODE_DEGRADED_LOCK,
		id,
		isLocked,
		isSealed,
		keySize, key,
		listId, stripeId,
		srcDataChunkId, dstDataChunkId,
		srcParityChunkId, dstParityChunkId
	);
	return this->buffer.send;
}

char *CoordinatorProtocol::resDegradedLock( size_t &size, uint32_t id, uint8_t keySize, char *key, uint32_t listId, uint32_t srcDataChunkId, uint32_t dstDataChunkId, uint32_t srcParityChunkId, uint32_t dstParityChunkId ) {
	// -- common/protocol/degraded_protocol.cc --
	size = this->generateDegradedLockResHeader(
		PROTO_MAGIC_RESPONSE_FAILURE,
		PROTO_MAGIC_TO_MASTER,
		PROTO_OPCODE_DEGRADED_LOCK,
		id,
		keySize, key,
		listId,
		srcDataChunkId, dstDataChunkId,
		srcParityChunkId, dstParityChunkId
	);
	return this->buffer.send;
}

char *CoordinatorProtocol::resDegradedLock( size_t &size, uint32_t id, bool exist, uint8_t keySize, char *key, uint32_t listId, uint32_t srcDataChunkId, uint32_t srcParityChunkId ) {
	// -- common/protocol/degraded_protocol.cc --
	size = this->generateDegradedLockResHeader(
		PROTO_MAGIC_RESPONSE_FAILURE,
		PROTO_MAGIC_TO_MASTER,
		PROTO_OPCODE_DEGRADED_LOCK,
		id,
		exist,
		keySize, key,
		listId, srcDataChunkId, srcParityChunkId
	);
	return this->buffer.send;
}

char *CoordinatorProtocol::reqReleaseDegradedLock( size_t &size, uint32_t id, std::vector<Metadata> &chunks, bool &isCompleted ) {
	// -- common/protocol/degraded_protocol.cc --
	size = this->generateDegradedReleaseReqHeader(
		PROTO_MAGIC_REQUEST,
		PROTO_MAGIC_TO_SLAVE,
		PROTO_OPCODE_RELEASE_DEGRADED_LOCKS,
		id,
		chunks,
		isCompleted
	);
	return this->buffer.send;
}

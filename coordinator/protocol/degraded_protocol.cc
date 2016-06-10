#include "protocol.hh"

char *CoordinatorProtocol::resDegradedLock(
	size_t &size, uint16_t instanceId, uint32_t requestId,
	bool isLocked, uint8_t keySize, char *key, bool isLarge,
	bool isSealed, uint32_t stripeId, uint32_t dataChunkId, uint32_t dataChunkCount,
	uint32_t *original, uint32_t *reconstructed, uint32_t reconstructedCount,
	uint32_t ongoingAtChunk, uint8_t numSurvivingChunkIds, uint32_t *survivingChunkIds
) {
	// -- common/protocol/degraded_protocol.cc --
	size = this->generateDegradedLockResHeader(
		isLocked ? PROTO_MAGIC_RESPONSE_SUCCESS : PROTO_MAGIC_RESPONSE_FAILURE,
		PROTO_MAGIC_TO_CLIENT,
		PROTO_OPCODE_DEGRADED_LOCK,
		instanceId, requestId,
		isLocked, keySize, key, isLarge,
		isSealed, stripeId, dataChunkId, dataChunkCount,
		original, reconstructed, reconstructedCount,
		ongoingAtChunk, numSurvivingChunkIds, survivingChunkIds
	);
	return this->buffer.send;
}

char *CoordinatorProtocol::resDegradedLock(
	size_t &size, uint16_t instanceId, uint32_t requestId,
	uint8_t keySize, char *key, bool isLarge,
	uint32_t *original, uint32_t *remapped, uint32_t remappedCount
) {
	// -- common/protocol/degraded_protocol.cc --
	size = this->generateDegradedLockResHeader(
		PROTO_MAGIC_RESPONSE_FAILURE,
		PROTO_MAGIC_TO_CLIENT,
		PROTO_OPCODE_DEGRADED_LOCK,
		instanceId, requestId,
		keySize, key, isLarge,
		original, remapped, remappedCount
	);
	return this->buffer.send;
}

char *CoordinatorProtocol::resDegradedLock(
	size_t &size, uint16_t instanceId, uint32_t requestId,
	bool exist,
	uint8_t keySize, char *key, bool isLarge
) {
	// -- common/protocol/degraded_protocol.cc --
	size = this->generateDegradedLockResHeader(
		PROTO_MAGIC_RESPONSE_FAILURE,
		PROTO_MAGIC_TO_CLIENT,
		PROTO_OPCODE_DEGRADED_LOCK,
		instanceId, requestId,
		exist,
		keySize, key, isLarge
	);
	return this->buffer.send;
}

char *CoordinatorProtocol::reqReleaseDegradedLock( size_t &size, uint16_t instanceId, uint32_t requestId, std::vector<Metadata> &chunks, bool &isCompleted ) {
	// -- common/protocol/degraded_protocol.cc --
	size = this->generateDegradedReleaseReqHeader(
		PROTO_MAGIC_REQUEST,
		PROTO_MAGIC_TO_SERVER,
		PROTO_OPCODE_RELEASE_DEGRADED_LOCKS,
		instanceId, requestId,
		chunks,
		isCompleted
	);
	return this->buffer.send;
}
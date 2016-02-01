#include "protocol.hh"

char *MasterProtocol::reqDegradedLock( size_t &size, uint16_t instanceId, uint32_t requestId, uint32_t *original, uint32_t *reconstructed, uint32_t reconstructedCount, char *key, uint8_t keySize ) {
	// -- common/protocol/degraded_protocol.cc --
	size = this->generateDegradedLockReqHeader(
		PROTO_MAGIC_REQUEST,
		PROTO_MAGIC_TO_COORDINATOR,
		PROTO_OPCODE_DEGRADED_LOCK,
		instanceId, requestId,
		original, reconstructed, reconstructedCount,
		keySize, key
	);
	return this->buffer.send;
}

char *MasterProtocol::reqDegradedGet( size_t &size, uint16_t instanceId, uint32_t requestId, bool isSealed, uint32_t stripeId, uint32_t *original, uint32_t *reconstructed, uint32_t reconstructedCount, uint32_t ongoingAtChunk, uint8_t numSurvivingChunkIds, uint32_t *survivingChunkIds, char *key, uint8_t keySize ) {
	// -- common/protocol/degraded_protocol.cc --
	size = this->generateDegradedReqHeader(
		PROTO_MAGIC_REQUEST,
		PROTO_MAGIC_TO_SLAVE,
		PROTO_OPCODE_DEGRADED_GET,
		instanceId, requestId,
		isSealed, stripeId,
		original, reconstructed, reconstructedCount,
		ongoingAtChunk, numSurvivingChunkIds, survivingChunkIds,
		keySize, key
	);
	return this->buffer.send;
}

char *MasterProtocol::reqDegradedUpdate( size_t &size, uint16_t instanceId, uint32_t requestId, bool isSealed, uint32_t stripeId, uint32_t *original, uint32_t *reconstructed, uint32_t reconstructedCount, uint32_t ongoingAtChunk, uint8_t numSurvivingChunkIds, uint32_t *survivingChunkIds, char *key, uint8_t keySize, char *valueUpdate, uint32_t valueUpdateOffset, uint32_t valueUpdateSize, uint32_t timestamp ) {
	// -- common/protocol/degraded_protocol.cc --
	size = this->generateDegradedReqHeader(
		PROTO_MAGIC_REQUEST,
		PROTO_MAGIC_TO_SLAVE,
		PROTO_OPCODE_DEGRADED_UPDATE,
		instanceId, requestId,
		isSealed, stripeId,
		original, reconstructed, reconstructedCount,
		ongoingAtChunk, numSurvivingChunkIds, survivingChunkIds,
		keySize, key,
		valueUpdateOffset, valueUpdateSize, valueUpdate,
		timestamp
	);
	return this->buffer.send;
}

char *MasterProtocol::reqDegradedDelete( size_t &size, uint16_t instanceId, uint32_t requestId, bool isSealed, uint32_t stripeId, uint32_t *original, uint32_t *reconstructed, uint32_t reconstructedCount, uint32_t ongoingAtChunk, uint8_t numSurvivingChunkIds, uint32_t *survivingChunkIds, char *key, uint8_t keySize, uint32_t timestamp ) {
	// -- common/protocol/degraded_protocol.cc --
	size = this->generateDegradedReqHeader(
		PROTO_MAGIC_REQUEST,
		PROTO_MAGIC_TO_SLAVE,
		PROTO_OPCODE_DEGRADED_DELETE,
		instanceId, requestId,
		isSealed, stripeId,
		original, reconstructed, reconstructedCount,
		ongoingAtChunk, numSurvivingChunkIds, survivingChunkIds,
		keySize, key,
		timestamp
	);
	return this->buffer.send;
}

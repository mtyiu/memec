#include "protocol.hh"

char *MasterProtocol::reqDegradedLock( size_t &size, uint32_t id, uint32_t listId, uint32_t srcDataChunkId, uint32_t dstDataChunkId, uint32_t srcParityChunkId, uint32_t dstParityChunkId, char *key, uint8_t keySize ) {
	// -- common/protocol/degraded_protocol.cc --
	size = this->generateDegradedLockReqHeader(
		PROTO_MAGIC_REQUEST,
		PROTO_MAGIC_TO_COORDINATOR,
		PROTO_OPCODE_DEGRADED_LOCK,
		id,
		listId,
		srcDataChunkId,
		dstDataChunkId,
		srcParityChunkId,
		dstParityChunkId,
		keySize,
		key
	);
	return this->buffer.send;
}

char *MasterProtocol::reqDegradedGet( size_t &size, uint32_t id, uint32_t listId, uint32_t stripeId, uint32_t srcDataChunkId, uint32_t dstDataChunkId, uint32_t srcParityChunkId, uint32_t dstParityChunkId, bool isSealed, char *key, uint8_t keySize ) {
	// -- common/protocol/degraded_protocol.cc --
	size = this->generateDegradedReqHeader(
		PROTO_MAGIC_REQUEST,
		PROTO_MAGIC_TO_SLAVE,
		PROTO_OPCODE_DEGRADED_GET,
		id,
		listId, stripeId,
		srcDataChunkId, dstDataChunkId,
		srcParityChunkId, dstParityChunkId,
		isSealed,
		keySize, key
	);
	return this->buffer.send;
}

char *MasterProtocol::reqDegradedUpdate( size_t &size, uint32_t id, uint32_t listId, uint32_t stripeId, uint32_t srcDataChunkId, uint32_t dstDataChunkId, uint32_t srcParityChunkId, uint32_t dstParityChunkId, bool isSealed, char *key, uint8_t keySize, char *valueUpdate, uint32_t valueUpdateOffset, uint32_t valueUpdateSize ) {
	// -- common/protocol/degraded_protocol.cc --
	size = this->generateDegradedReqHeader(
		PROTO_MAGIC_REQUEST,
		PROTO_MAGIC_TO_SLAVE,
		PROTO_OPCODE_DEGRADED_UPDATE,
		id,
		listId, stripeId,
		srcDataChunkId, dstDataChunkId,
		srcParityChunkId, dstParityChunkId,
		isSealed,
		keySize, key,
		valueUpdateOffset, valueUpdateSize, valueUpdate
	);
	return this->buffer.send;
}

char *MasterProtocol::reqDegradedDelete( size_t &size, uint32_t id, uint32_t listId, uint32_t stripeId, uint32_t srcDataChunkId, uint32_t dstDataChunkId, uint32_t srcParityChunkId, uint32_t dstParityChunkId, bool isSealed, char *key, uint8_t keySize ) {
	// -- common/protocol/degraded_protocol.cc --
	size = this->generateDegradedReqHeader(
		PROTO_MAGIC_REQUEST,
		PROTO_MAGIC_TO_SLAVE,
		PROTO_OPCODE_DEGRADED_DELETE,
		id,
		listId, stripeId,
		srcDataChunkId, dstDataChunkId,
		srcParityChunkId, dstParityChunkId,
		isSealed,
		keySize, key
	);
	return this->buffer.send;
}

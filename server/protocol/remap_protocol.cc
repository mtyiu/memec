#include "protocol.hh"

char *ServerProtocol::resDegradedSet(
	size_t &size, bool toClient,
	uint16_t instanceId, uint32_t requestId, bool success,
	uint32_t listId, uint32_t chunkId,
	uint32_t *original, uint32_t *remapped, uint32_t remappedCount,
	uint8_t keySize, char *key
) {
	// -- common/protocol/remap_protocol.cc --
	size = this->generateDegradedSetHeader(
		success ? PROTO_MAGIC_RESPONSE_SUCCESS : PROTO_MAGIC_RESPONSE_FAILURE,
		toClient ? PROTO_MAGIC_TO_CLIENT : PROTO_MAGIC_TO_SERVER,
		PROTO_OPCODE_DEGRADED_SET,
		instanceId, requestId,
		listId, chunkId,
		original, remapped, remappedCount,
		keySize, key,
		0, // valueSize
		0  // value
	);
	return this->buffer.send;
}

char *ServerProtocol::resRemapParity( size_t &size, uint16_t instanceId, uint32_t requestId ) {
	// -- common/protocol/protocol.cc --
	size = this->generateHeader(
		PROTO_MAGIC_RESPONSE_SUCCESS,
		PROTO_MAGIC_TO_COORDINATOR,
		PROTO_OPCODE_PARITY_MIGRATE,
		0,
		instanceId, requestId
	);
	return this->buffer.send;
};

char *ServerProtocol::reqSet( size_t &size, uint16_t instanceId, uint32_t requestId, char *key, uint8_t keySize, char *value, uint32_t valueSize, char *buf ) {
	// -- common/protocol/normal_protocol.cc --
	if ( ! buf ) buf = this->buffer.send;
	size = this->generateKeyValueHeader(
		PROTO_MAGIC_REQUEST,
		PROTO_MAGIC_TO_SERVER,
		PROTO_OPCODE_SET,
		instanceId, requestId,
		keySize,
		key,
		valueSize,
		value,
		buf
	);
	return buf;
}

char *ServerProtocol::reqRemappedUpdate( size_t &size, uint16_t instanceId, uint32_t requestId, char *key, uint8_t keySize, char *valueUpdate, uint32_t valueUpdateOffset, uint32_t valueUpdateSize, char *buf, uint32_t timestamp ) {
	// -- common/protocol/normal_protocol.cc --
	if ( ! buf ) buf = this->buffer.send;
	size = this->generateKeyValueUpdateHeader(
		PROTO_MAGIC_REQUEST,
		PROTO_MAGIC_TO_SERVER,
		PROTO_OPCODE_REMAPPED_UPDATE,
		instanceId, requestId,
		keySize,
		key,
		valueUpdateOffset,
		valueUpdateSize,
		valueUpdate,
		buf, timestamp
	);
	return buf;
}

char *ServerProtocol::reqRemappedDelete( size_t &size, uint16_t instanceId, uint32_t requestId, char *key, uint8_t keySize, char *buf, uint32_t timestamp ) {
	// -- common/protocol/normal_protocol.cc --
	if ( ! buf ) buf = this->buffer.send;
	size = this->generateKeyHeader(
		PROTO_MAGIC_REQUEST,
		PROTO_MAGIC_TO_SERVER,
		PROTO_OPCODE_REMAPPED_DELETE,
		instanceId, requestId,
		keySize,
		key,
		buf, timestamp
	);
	return buf;
}

char *ServerProtocol::resRemappedUpdate( size_t &size, uint16_t instanceId, uint32_t requestId, bool success, char *key, uint8_t keySize, uint32_t valueUpdateOffset, uint32_t valueUpdateSize, char *buf, uint32_t timestamp ) {
	// -- common/protocol/normal_protocol.cc --
	if ( ! buf ) buf = this->buffer.send;
	size = this->generateKeyValueUpdateHeader(
		success ? PROTO_MAGIC_RESPONSE_SUCCESS : PROTO_MAGIC_RESPONSE_FAILURE,
		PROTO_MAGIC_TO_SERVER,
		PROTO_OPCODE_REMAPPED_UPDATE,
		instanceId, requestId,
		keySize, key,
		valueUpdateOffset, valueUpdateSize, 0,
		buf, timestamp
	);
	return buf;
}

char *ServerProtocol::resRemappedDelete( size_t &size, uint16_t instanceId, uint32_t requestId, bool success, char *key, uint8_t keySize, char *buf, uint32_t timestamp ) {
	// -- common/protocol/normal_protocol.cc --
	if ( ! buf ) buf = this->buffer.send;
	size = this->generateKeyHeader(
		success ? PROTO_MAGIC_RESPONSE_SUCCESS : PROTO_MAGIC_RESPONSE_FAILURE,
		PROTO_MAGIC_TO_SERVER,
		PROTO_OPCODE_REMAPPED_DELETE,
		instanceId, requestId,
		keySize, key,
		buf, timestamp
	);
	return buf;
}

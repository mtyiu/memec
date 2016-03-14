#include "protocol.hh"

char *ClientProtocol::replaySet( size_t &size, uint16_t instanceId, uint32_t requestId, char *key, uint8_t keySize, char *value, uint32_t valueSize ) {
	size = this->generateKeyValueHeader(
		PROTO_MAGIC_REQUEST,
		PROTO_MAGIC_TO_CLIENT,
		PROTO_OPCODE_SET,
		instanceId, requestId,
		keySize,
		key,
		valueSize,
		value,
		this->buffer.recv
	);
	return this->buffer.recv;
}

char *ClientProtocol::replayGet( size_t &size, uint16_t instanceId, uint32_t requestId, char *key, uint8_t keySize ) {
	size = this->generateKeyHeader(
		PROTO_MAGIC_REQUEST,
		PROTO_MAGIC_TO_CLIENT,
		PROTO_OPCODE_GET,
		instanceId, requestId,
		keySize,
		key,
		this->buffer.recv
	);
	return this->buffer.recv;
}

char *ClientProtocol::replayUpdate( size_t &size, uint16_t instanceId, uint32_t requestId, char *key, uint8_t keySize, char *valueUpdate, uint32_t valueUpdateOffset, uint32_t valueUpdateSize) {
	size = this->generateKeyValueUpdateHeader(
		PROTO_MAGIC_REQUEST,
		PROTO_MAGIC_TO_CLIENT,
		PROTO_OPCODE_SET,
		instanceId, requestId,
		keySize,
		key,
		valueUpdateOffset,
		valueUpdateSize,
		valueUpdate,
		this->buffer.recv
	);
	return this->buffer.recv;
}

char *ClientProtocol::replayDelete( size_t &size, uint16_t instanceId, uint32_t requestId, char *key, uint8_t keySize ) {
	size = this->generateKeyHeader(
		PROTO_MAGIC_REQUEST,
		PROTO_MAGIC_TO_CLIENT,
		PROTO_OPCODE_DELETE,
		instanceId, requestId,
		keySize,
		key,
		this->buffer.recv
	);
	return this->buffer.recv;
}

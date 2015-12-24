#include "protocol.hh"

char *ApplicationProtocol::reqRegisterMaster( size_t &size, uint32_t requestId ) {
	// -- common/protocol/protocol.cc --
	size = this->generateHeader(
		PROTO_MAGIC_REQUEST,
		PROTO_MAGIC_TO_MASTER,
		PROTO_OPCODE_REGISTER,
		0, // length
		PROTO_UNINITIALIZED_INSTANCE, requestId
	);
	return this->buffer.send;
}

char *ApplicationProtocol::reqSet( size_t &size, uint16_t instanceId, uint32_t requestId, char *key, uint8_t keySize, char *value, uint32_t valueSize ) {
	// -- common/protocol/normal_protocol.cc --
	size = this->generateKeyValueHeader(
		PROTO_MAGIC_REQUEST,
		PROTO_MAGIC_TO_MASTER,
		PROTO_OPCODE_SET,
		instanceId, requestId,
		keySize,
		key,
		valueSize,
		value
	);
	return this->buffer.send;
}

char *ApplicationProtocol::reqGet( size_t &size, uint16_t instanceId, uint32_t requestId, char *key, uint8_t keySize ) {
	// -- common/protocol/normal_protocol.cc --
	size = this->generateKeyHeader(
		PROTO_MAGIC_REQUEST,
		PROTO_MAGIC_TO_MASTER,
		PROTO_OPCODE_GET,
		instanceId, requestId,
		keySize,
		key
	);
	return this->buffer.send;
}

char *ApplicationProtocol::reqUpdate( size_t &size, uint16_t instanceId, uint32_t requestId, char *key, uint8_t keySize, char *valueUpdate, uint32_t valueUpdateOffset, uint32_t valueUpdateSize ) {
	// -- common/protocol/normal_protocol.cc --
	size = this->generateKeyValueUpdateHeader(
		PROTO_MAGIC_REQUEST,
		PROTO_MAGIC_TO_MASTER,
		PROTO_OPCODE_UPDATE,
		instanceId, requestId,
		keySize,
		key,
		valueUpdateOffset,
		valueUpdateSize,
		valueUpdate
	);
	return this->buffer.send;
}

char *ApplicationProtocol::reqDelete( size_t &size, uint16_t instanceId, uint32_t requestId, char *key, uint8_t keySize ) {
	// -- common/protocol/normal_protocol.cc --
	size = this->generateKeyHeader(
		PROTO_MAGIC_REQUEST,
		PROTO_MAGIC_TO_MASTER,
		PROTO_OPCODE_DELETE,
		instanceId, requestId,
		keySize,
		key
	);
	return this->buffer.send;
}

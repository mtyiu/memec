#include "protocol.hh"

char *ApplicationProtocol::reqRegisterMaster( size_t &size, uint32_t id ) {
	size = this->generateHeader(
		PROTO_MAGIC_REQUEST,
		PROTO_MAGIC_TO_MASTER,
		PROTO_OPCODE_REGISTER,
		0, // length
		id
	);
	return this->buffer.send;
}

char *ApplicationProtocol::reqSet( size_t &size, uint32_t id, char *key, uint8_t keySize, char *value, uint32_t valueSize ) {
	size = this->generateKeyValueHeader(
		PROTO_MAGIC_REQUEST,
		PROTO_MAGIC_TO_MASTER,
		PROTO_OPCODE_SET,
		id,
		keySize,
		key,
		valueSize,
		value
	);
	return this->buffer.send;
}

char *ApplicationProtocol::reqGet( size_t &size, uint32_t id, char *key, uint8_t keySize ) {
	size = this->generateKeyHeader(
		PROTO_MAGIC_REQUEST,
		PROTO_MAGIC_TO_MASTER,
		PROTO_OPCODE_GET,
		id,
		keySize,
		key
	);
	return this->buffer.send;
}

char *ApplicationProtocol::reqUpdate( size_t &size, uint32_t id, char *key, uint8_t keySize, char *valueUpdate, uint32_t valueUpdateOffset, uint32_t valueUpdateSize ) {
	size = this->generateKeyValueUpdateHeader(
		PROTO_MAGIC_REQUEST,
		PROTO_MAGIC_TO_MASTER,
		PROTO_OPCODE_UPDATE,
		id,
		keySize,
		key,
		valueUpdateOffset,
		valueUpdateSize,
		valueUpdate
	);
	return this->buffer.send;
}

char *ApplicationProtocol::reqDelete( size_t &size, uint32_t id, char *key, uint8_t keySize ) {
	size = this->generateKeyHeader(
		PROTO_MAGIC_REQUEST,
		PROTO_MAGIC_TO_MASTER,
		PROTO_OPCODE_DELETE,
		id,
		keySize,
		key
	);
	return this->buffer.send;
}

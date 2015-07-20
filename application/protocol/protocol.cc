#include "protocol.hh"

char *ApplicationProtocol::reqRegisterMaster( size_t &size ) {
	size = this->generateHeader(
		PROTO_MAGIC_REQUEST,
		PROTO_MAGIC_TO_MASTER,
		PROTO_OPCODE_REGISTER,
		0
	);
	return this->buffer.data;
}

char *ApplicationProtocol::reqSet( size_t &size, char *key, uint8_t keySize, char *value, uint32_t valueSize ) {
	size = this->generateKeyValuePacket(
		PROTO_MAGIC_REQUEST,
		PROTO_MAGIC_TO_MASTER,
		PROTO_OPCODE_SET,
		keySize,
		key,
		valueSize,
		value
	);
	return this->buffer.data;
}

char *ApplicationProtocol::reqGet( size_t &size, char *key, uint8_t keySize ) {
	size = this->generateKeyValuePacket(
		PROTO_MAGIC_REQUEST,
		PROTO_MAGIC_TO_MASTER,
		PROTO_OPCODE_GET,
		keySize,
		key
	);
	return this->buffer.data;
}

#include "protocol.hh"

char *MasterProtocol::reqRegisterCoordinator( size_t &size ) {
	size = this->generateHeader(
		PROTO_MAGIC_REQUEST,
		PROTO_MAGIC_TO_COORDINATOR,
		PROTO_OPCODE_REGISTER,
		0
	);
	return this->buffer.data;
}

char *MasterProtocol::reqRegisterSlave( size_t &size ) {
	size = this->generateHeader(
		PROTO_MAGIC_REQUEST,
		PROTO_MAGIC_TO_SLAVE,
		PROTO_OPCODE_REGISTER,
		0
	);
	return this->buffer.data;
}

char *MasterProtocol::resRegisterApplication( size_t &size, bool success ) {
	size = this->generateHeader(
		success ? PROTO_MAGIC_RESPONSE_SUCCESS : PROTO_MAGIC_RESPONSE_FAILURE,
		PROTO_MAGIC_TO_APPLICATION,
		PROTO_OPCODE_REGISTER,
		0
	);
	return this->buffer.data;
}

char *MasterProtocol::resSet( size_t &size, bool success, uint8_t keySize, char *key ) {
	size = this->generateKeyValuePacket(
		success ? PROTO_MAGIC_RESPONSE_SUCCESS : PROTO_MAGIC_RESPONSE_FAILURE,
		PROTO_MAGIC_TO_APPLICATION,
		PROTO_OPCODE_SET,
		keySize,
		key
	);
	return this->buffer.data;
}

char *MasterProtocol::resGet( size_t &size, bool success, uint8_t keySize, char *key, uint32_t valueSize, char *value ) {
	size = this->generateKeyValuePacket(
		success ? PROTO_MAGIC_RESPONSE_SUCCESS : PROTO_MAGIC_RESPONSE_FAILURE,
		PROTO_MAGIC_TO_APPLICATION,
		PROTO_OPCODE_GET,
		keySize,
		key,
		valueSize,
		value
	);
	return this->buffer.data;
}

#include "protocol.hh"

char *ClientProtocol::reqRegisterCoordinator( size_t &size, uint32_t requestId, uint32_t addr, uint16_t port ) {
	// -- common/protocol/address_protocol.cc --
	size = this->generateAddressHeader(
		PROTO_MAGIC_REQUEST,
		PROTO_MAGIC_TO_COORDINATOR,
		PROTO_OPCODE_REGISTER,
		PROTO_UNINITIALIZED_INSTANCE, requestId,
		addr, port
	);
	return this->buffer.send;
}

char *ClientProtocol::reqRegisterServer( size_t &size, uint16_t instanceId, uint32_t requestId, uint32_t addr, uint16_t port ) {
	// -- common/protocol/address_protocol.cc --
	size = this->generateAddressHeader(
		PROTO_MAGIC_REQUEST,
		PROTO_MAGIC_TO_SERVER,
		PROTO_OPCODE_REGISTER,
		instanceId, requestId,
		addr,
		port
	);
	return this->buffer.send;
}

char *ClientProtocol::resRegisterApplication( size_t &size, uint16_t instanceId, uint32_t requestId, bool success ) {
	// -- common/protocol/protocol.cc --
	size = this->generateHeader(
		success ? PROTO_MAGIC_RESPONSE_SUCCESS : PROTO_MAGIC_RESPONSE_FAILURE,
		PROTO_MAGIC_TO_APPLICATION,
		PROTO_OPCODE_REGISTER,
		0, // length
		instanceId, requestId
	);
	return this->buffer.send;
}

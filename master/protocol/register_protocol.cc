#include "protocol.hh"

char *MasterProtocol::reqRegisterCoordinator( size_t &size, uint32_t id, uint32_t addr, uint16_t port ) {
	// -- common/protocol/address_protocol.cc --
	size = this->generateAddressHeader(
		PROTO_MAGIC_REQUEST,
		PROTO_MAGIC_TO_COORDINATOR,
		PROTO_OPCODE_REGISTER,
		id,
		addr, port
	);
	return this->buffer.send;
}

char *MasterProtocol::reqRegisterSlave( size_t &size, uint32_t id, uint32_t addr, uint16_t port ) {
	// -- common/protocol/address_protocol.cc --
	size = this->generateAddressHeader(
		PROTO_MAGIC_REQUEST,
		PROTO_MAGIC_TO_SLAVE,
		PROTO_OPCODE_REGISTER,
		id,
		addr,
		port
	);
	return this->buffer.send;
}

char *MasterProtocol::resRegisterApplication( size_t &size, uint32_t id, bool success ) {
	// -- common/protocol/protocol.cc --
	size = this->generateHeader(
		success ? PROTO_MAGIC_RESPONSE_SUCCESS : PROTO_MAGIC_RESPONSE_FAILURE,
		PROTO_MAGIC_TO_APPLICATION,
		PROTO_OPCODE_REGISTER,
		0, // length
		id
	);
	return this->buffer.send;
}

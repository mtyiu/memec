#include "protocol.hh"

char *CoordinatorProtocol::resRegisterMaster( size_t &size, uint32_t id, bool success ) {
	size = this->generateHeader(
		success ? PROTO_MAGIC_RESPONSE_SUCCESS : PROTO_MAGIC_RESPONSE_FAILURE,
		PROTO_MAGIC_TO_MASTER,
		PROTO_OPCODE_REGISTER,
		0, // length
		id
	);
	return this->buffer.send;
}

char *CoordinatorProtocol::resRegisterSlave( size_t &size, uint32_t id, bool success ) {
	// -- common/protocol/protocol.cc --
	size = this->generateHeader(
		success ? PROTO_MAGIC_RESPONSE_SUCCESS : PROTO_MAGIC_RESPONSE_FAILURE,
		PROTO_MAGIC_TO_SLAVE,
		PROTO_OPCODE_REGISTER,
		0, // length
		id
	);
	return this->buffer.send;
}

char *CoordinatorProtocol::announceSlaveConnected( size_t &size, uint32_t id, SlaveSocket *socket ) {
	// -- common/protocol/address_protocol.cc --
	ServerAddr addr = socket->getServerAddr();
	size = this->generateAddressHeader(
		PROTO_MAGIC_ANNOUNCEMENT,
		PROTO_MAGIC_TO_SLAVE,
		PROTO_OPCODE_SLAVE_CONNECTED,
		id,
		addr.addr,
		addr.port
	);
	return this->buffer.send;
}

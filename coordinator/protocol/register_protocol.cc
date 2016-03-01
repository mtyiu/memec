#include "protocol.hh"

char *CoordinatorProtocol::resRegisterMaster( size_t &size, uint16_t instanceId, uint32_t requestId, bool success ) {
	size = this->generateHeader(
		success ? PROTO_MAGIC_RESPONSE_SUCCESS : PROTO_MAGIC_RESPONSE_FAILURE,
		PROTO_MAGIC_TO_CLIENT,
		PROTO_OPCODE_REGISTER,
		0, // length
		instanceId, requestId
	);
	return this->buffer.send;
}

char *CoordinatorProtocol::resRegisterSlave( size_t &size, uint16_t instanceId, uint32_t requestId, bool success ) {
	// -- common/protocol/protocol.cc --
	size = this->generateHeader(
		success ? PROTO_MAGIC_RESPONSE_SUCCESS : PROTO_MAGIC_RESPONSE_FAILURE,
		PROTO_MAGIC_TO_SERVER,
		PROTO_OPCODE_REGISTER,
		0, // length
		instanceId, requestId
	);
	return this->buffer.send;
}

char *CoordinatorProtocol::announceSlaveConnected( size_t &size, uint16_t instanceId, uint32_t requestId, SlaveSocket *socket ) {
	// -- common/protocol/address_protocol.cc --
	ServerAddr addr = socket->getServerAddr();
	size = this->generateAddressHeader(
		PROTO_MAGIC_ANNOUNCEMENT,
		PROTO_MAGIC_TO_SERVER,
		PROTO_OPCODE_SERVER_CONNECTED,
		instanceId, requestId,
		addr.addr,
		addr.port
	);
	return this->buffer.send;
}

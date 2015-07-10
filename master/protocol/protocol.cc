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

char *MasterProtocol::resRegisterApplication( size_t &size, bool success ) {
	size = this->generateHeader(
		success ? PROTO_MAGIC_RESPONSE_SUCCESS : PROTO_MAGIC_RESPONSE_FAILURE,
		PROTO_MAGIC_TO_APPLICATION,
		PROTO_OPCODE_REGISTER,
		0
	);
	return this->buffer.data;
}

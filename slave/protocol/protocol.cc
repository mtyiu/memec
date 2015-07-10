#include "protocol.hh"

char *SlaveProtocol::reqRegisterCoordinator( size_t &size ) {
	size = this->generateHeader(
		PROTO_MAGIC_REQUEST,
		PROTO_MAGIC_TO_COORDINATOR,
		PROTO_OPCODE_REGISTER,
		0
	);
	return this->buffer.data;
}
#include "protocol.hh"

char *CoordinatorProtocol::resHeartbeat( size_t &size, uint32_t id, uint32_t timestamp, uint32_t sealed, uint32_t keys, bool isLast ) {
	size = this->generateHeartbeatMessage(
		PROTO_MAGIC_RESPONSE_SUCCESS,
		PROTO_MAGIC_TO_SLAVE,
		PROTO_OPCODE_SYNC,
		id,
		timestamp,
		sealed,
		keys,
		isLast
	);
	return this->buffer.send;
}

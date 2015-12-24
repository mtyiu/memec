#include "protocol.hh"

char *CoordinatorProtocol::resHeartbeat( size_t &size, uint16_t instanceId, uint32_t requestId, uint32_t timestamp, uint32_t sealed, uint32_t keys, bool isLast ) {
	size = this->generateHeartbeatMessage(
		PROTO_MAGIC_RESPONSE_SUCCESS,
		PROTO_MAGIC_TO_SLAVE,
		PROTO_OPCODE_SYNC,
		instanceId, requestId,
		timestamp,
		sealed,
		keys,
		isLast
	);
	return this->buffer.send;
}

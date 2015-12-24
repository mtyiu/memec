#include "protocol.hh"

char *SlaveProtocol::ackMetadata( size_t &size, uint16_t instanceId, uint32_t requestId, uint32_t fromTimestamp, uint32_t toTimestamp ) {
	size = this->generateAcknowledgementHeader(
		PROTO_MAGIC_ACKNOWLEDGEMENT,
		PROTO_MAGIC_TO_MASTER,
		PROTO_OPCODE_ACK_METADATA,
		instanceId, requestId,
		fromTimestamp, toTimestamp
	);
	return this->buffer.send;
}

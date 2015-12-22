#include "protocol.hh"

char *SlaveProtocol::ackMetadata( size_t &size, uint32_t id, uint32_t fromTimestamp, uint32_t toTimestamp ) {
	size = this->generateAcknowledgementHeader(
		PROTO_MAGIC_ACKNOWLEDGEMENT,
		PROTO_MAGIC_TO_MASTER,
		PROTO_OPCODE_ACK_METADATA,
		id, fromTimestamp, toTimestamp
	);
	return this->buffer.send;
}

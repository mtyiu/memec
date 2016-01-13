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

char *SlaveProtocol::ackParityDeltaBackup( size_t &size, uint16_t instanceId, uint32_t requestId, std::vector<uint32_t> timestamps, uint16_t targetId ) {
	size = this->generateDeltaAcknowledgementHeader(
		PROTO_MAGIC_ACKNOWLEDGEMENT,
		PROTO_MAGIC_TO_MASTER,
		PROTO_OPCODE_ACK_PARITY_DELTA,
		instanceId, requestId,
		timestamps,
		targetId
	);
	return this->buffer.send;
}

char *SlaveProtocol::resRevertParityDelta( size_t &size, uint16_t instanceId, uint32_t requestId, bool success, std::vector<uint32_t> timestamps, uint16_t targetId ) {
	size = this->generateDeltaAcknowledgementHeader(
		success ? PROTO_MAGIC_RESPONSE_SUCCESS : PROTO_MAGIC_RESPONSE_FAILURE,
		PROTO_MAGIC_TO_MASTER,
		PROTO_OPCODE_REVERT_PARITY_DELTA,
		instanceId, requestId,
		timestamps,
		targetId
	);
	return this->buffer.send;
}

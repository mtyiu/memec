#include "protocol.hh"

char *ClientProtocol::syncMetadataBackup(
	size_t &size, uint16_t instanceId, uint32_t requestId,
	uint32_t addr, uint16_t port,
	LOCK_T *lock,
	std::unordered_multimap<uint32_t, Metadata> &sealed, uint32_t &sealedCount,
	std::unordered_map<Key, MetadataBackup> &ops, uint32_t &opsCount,
	bool &isCompleted
) {
	// -- common/protocol/fault_protocol.cc --
	size = this->generateMetadataBackupMessage(
		PROTO_MAGIC_HEARTBEAT,
		PROTO_MAGIC_TO_COORDINATOR,
		PROTO_OPCODE_SYNC,
		instanceId, requestId,
		addr, port,
		lock,
		sealed, sealedCount,
		ops, opsCount,
		isCompleted
	);
	return this->buffer.send;
}

char *ClientProtocol::ackParityDeltaBackup(
	size_t &size, uint16_t instanceId, uint32_t requestId,
	std::vector<uint32_t> timestamps,
	uint16_t targetId
) {
	size = this->generateDeltaAcknowledgementHeader(
		PROTO_MAGIC_REQUEST,
		PROTO_MAGIC_TO_SERVER,
		PROTO_OPCODE_ACK_PARITY_DELTA,
		instanceId, requestId,
		timestamps,
		std::vector<Key>(),
		targetId
	);

	return this->buffer.send;
}

char *ClientProtocol::revertDelta(
	size_t &size, uint16_t instanceId, uint32_t requestId,
	std::vector<uint32_t> timestamps,
	std::vector<Key> requests,
	uint16_t targetId
) {
	size = this->generateDeltaAcknowledgementHeader(
		PROTO_MAGIC_REQUEST,
		PROTO_MAGIC_TO_SERVER,
		PROTO_OPCODE_REVERT_DELTA,
		instanceId, requestId,
		timestamps,
		requests,
		targetId
	);

	return this->buffer.send;
}

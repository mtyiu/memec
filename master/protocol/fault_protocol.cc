#include "protocol.hh"

char *MasterProtocol::syncMetadataBackup(
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

char *MasterProtocol::ackParityDeltaBackup(
	size_t &size, uint16_t instanceId, uint32_t requestId,
	uint32_t fromTimestamp, uint32_t toTimestamp,
	uint16_t targetId
) {
	size = this->generateParityDeltaAcknowledgementHeader(
		PROTO_MAGIC_REQUEST,
		PROTO_MAGIC_TO_SLAVE,
		PROTO_OPCODE_ACK_PARITY_DELTA,
		instanceId, requestId,
		fromTimestamp, toTimestamp,
		targetId
	);
	return this->buffer.send;
}

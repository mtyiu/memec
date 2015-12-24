#include "protocol.hh"

char *SlaveProtocol::sendHeartbeat(
	size_t &size, uint16_t instanceId, uint32_t requestId, uint32_t timestamp,
	LOCK_T *sealedLock, std::unordered_set<Metadata> &sealed, uint32_t &sealedCount,
	LOCK_T *opsLock, std::unordered_map<Key, OpMetadata> &ops, uint32_t &opsCount,
	bool &isCompleted
) {
	// -- common/protocol/heartbeat_protocol.cc --
	size = this->generateHeartbeatMessage(
		PROTO_MAGIC_HEARTBEAT,
		PROTO_MAGIC_TO_COORDINATOR,
		PROTO_OPCODE_SYNC,
		instanceId, requestId, timestamp,
		sealedLock, sealed, sealedCount,
		opsLock, ops, opsCount,
		isCompleted
	);
	return this->buffer.send;
}

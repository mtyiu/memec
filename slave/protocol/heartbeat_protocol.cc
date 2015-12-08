#include "protocol.hh"

char *SlaveProtocol::sendHeartbeat(
	size_t &size, uint32_t id,
	LOCK_T *sealedLock, std::unordered_set<Metadata> &sealed, uint32_t &sealedCount,
	LOCK_T *opsLock, std::unordered_map<Key, OpMetadata> &ops, uint32_t &opsCount,
	bool &isCompleted
) {
	// -- common/protocol/heartbeat_protocol.cc --
	size = this->generateHeartbeatMessage(
		PROTO_MAGIC_HEARTBEAT,
		PROTO_MAGIC_TO_COORDINATOR,
		PROTO_OPCODE_SYNC,
		id,
		sealedLock, sealed, sealedCount,
		opsLock, ops, opsCount,
		isCompleted
	);
	return this->buffer.send;
}

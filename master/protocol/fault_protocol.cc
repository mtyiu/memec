#include "protocol.hh"

char *MasterProtocol::syncMetadataBackup(
	size_t &size, uint32_t id, LOCK_T *lock,
	std::unordered_multimap<uint32_t, Metadata> &sealed, uint32_t &sealedCount,
	std::unordered_map<Key, MetadataBackup> &ops, uint32_t &opsCount,
	bool &isCompleted
) {
	// -- common/protocol/fault_protocol.cc --
	size = this->generateHeartbeatMessage(
		PROTO_MAGIC_HEARTBEAT,
		PROTO_MAGIC_TO_COORDINATOR,
		PROTO_OPCODE_SYNC,
		id, lock,
		sealed, sealedCount,
		ops, opsCount,
		isCompleted
	);
	return this->buffer.send;
}

#include "protocol.hh"

char *SlaveProtocol::resReconstruction( size_t &size, uint32_t id, uint32_t listId, uint32_t chunkId, uint32_t numStripes ) {
	// -- common/protocol/recovery_protocol.cc --
	size = this->generateReconstructionHeader(
		PROTO_MAGIC_RESPONSE_SUCCESS,
		PROTO_MAGIC_TO_COORDINATOR,
		PROTO_OPCODE_RECONSTRUCTION,
		id,
		listId, chunkId, numStripes
	);
	return this->buffer.send;
}

char *SlaveProtocol::resPromoteBackupSlave( size_t &size, uint32_t id, uint32_t addr, uint16_t port, uint32_t numStripes ) {
	// -- common/protocol/recovery_protocol.cc --
	size = this->generatePromoteBackupSlaveHeader(
		PROTO_MAGIC_RESPONSE_SUCCESS,
		PROTO_MAGIC_TO_COORDINATOR,
		PROTO_OPCODE_BACKUP_SLAVE_PROMOTED,
		id,
		addr, port, numStripes
	);
	return this->buffer.send;
}
#include "protocol.hh"

char *CoordinatorProtocol::announceSlaveReconstructed( size_t &size, uint32_t id, SlaveSocket *srcSocket, SlaveSocket *dstSocket ) {
	// -- common/protocol/address_protocol.cc --
	ServerAddr srcAddr = srcSocket->getServerAddr(), dstAddr = dstSocket->getServerAddr();
	size = this->generateSrcDstAddressHeader(
		PROTO_MAGIC_ANNOUNCEMENT,
		PROTO_MAGIC_TO_SLAVE,
		PROTO_OPCODE_SLAVE_RECONSTRUCTED,
		id,
		srcAddr.addr,
		srcAddr.port,
		dstAddr.addr,
		dstAddr.port
	);
	return this->buffer.send;
}

char *CoordinatorProtocol::promoteBackupSlave( size_t &size, uint32_t id, SlaveSocket *srcSocket, std::unordered_set<Metadata> &chunks, std::unordered_set<Metadata>::iterator &it, bool &isCompleted ) {
	// -- common/protocol/recovery_protocol.cc --
	ServerAddr srcAddr = srcSocket->getServerAddr();
	size = this->generatePromoteBackupSlaveHeader(
		PROTO_MAGIC_ANNOUNCEMENT,
		PROTO_MAGIC_TO_SLAVE,
		PROTO_OPCODE_BACKUP_SLAVE_PROMOTED,
		id,
		srcAddr.addr,
		srcAddr.port,
		chunks, it, isCompleted
	);
	return this->buffer.send;
}

char *CoordinatorProtocol::reqReconstruction( size_t &size, uint32_t id, uint32_t listId, uint32_t chunkId, std::unordered_set<uint32_t> &stripeIds, std::unordered_set<uint32_t>::iterator &it, uint32_t numChunks, bool &isCompleted ) {
	// -- common/protocol/recovery_protocol.cc --
	size = this->generateReconstructionHeader(
		PROTO_MAGIC_REQUEST,
		PROTO_MAGIC_TO_SLAVE,
		PROTO_OPCODE_RECONSTRUCTION,
		id,
		listId,
		chunkId,
		stripeIds,
		it,
		numChunks,
		isCompleted
	);
	return this->buffer.send;
}

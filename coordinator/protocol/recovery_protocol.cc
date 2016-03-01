#include "protocol.hh"

char *CoordinatorProtocol::announceSlaveReconstructed( size_t &size, uint16_t instanceId, uint32_t requestId, SlaveSocket *srcSocket, SlaveSocket *dstSocket, bool toSlave ) {
	// -- common/protocol/address_protocol.cc --
	ServerAddr srcAddr = srcSocket->getServerAddr(), dstAddr = dstSocket->getServerAddr();
	size = this->generateSrcDstAddressHeader(
		PROTO_MAGIC_ANNOUNCEMENT,
		toSlave ? PROTO_MAGIC_TO_SERVER : PROTO_MAGIC_TO_CLIENT,
		PROTO_OPCODE_SERVER_RECONSTRUCTED,
		instanceId, requestId,
		srcAddr.addr,
		srcAddr.port,
		dstAddr.addr,
		dstAddr.port
	);
	return this->buffer.send;
}

char *CoordinatorProtocol::promoteBackupSlave( size_t &size, uint16_t instanceId, uint32_t requestId, SlaveSocket *srcSocket, std::unordered_set<Metadata> &chunks, std::unordered_set<Metadata>::iterator &chunksIt, std::unordered_set<Key> &keys, std::unordered_set<Key>::iterator &keysIt, bool &isCompleted ) {
	// -- common/protocol/recovery_protocol.cc --
	ServerAddr srcAddr = srcSocket->getServerAddr();
	size = this->generatePromoteBackupSlaveHeader(
		PROTO_MAGIC_ANNOUNCEMENT,
		PROTO_MAGIC_TO_SERVER,
		PROTO_OPCODE_BACKUP_SERVER_PROMOTED,
		instanceId, requestId,
		srcAddr.addr,
		srcAddr.port,
		chunks, chunksIt,
		keys, keysIt,
		isCompleted
	);
	return this->buffer.send;
}

char *CoordinatorProtocol::reqReconstruction( size_t &size, uint16_t instanceId, uint32_t requestId, uint32_t listId, uint32_t chunkId, std::unordered_set<uint32_t> &stripeIds, std::unordered_set<uint32_t>::iterator &it, uint32_t numChunks, bool &isCompleted ) {
	// -- common/protocol/recovery_protocol.cc --
	size = this->generateReconstructionHeader(
		PROTO_MAGIC_REQUEST,
		PROTO_MAGIC_TO_SERVER,
		PROTO_OPCODE_RECONSTRUCTION,
		instanceId, requestId,
		listId,
		chunkId,
		stripeIds,
		it,
		numChunks,
		isCompleted
	);
	return this->buffer.send;
}

char *CoordinatorProtocol::reqReconstructionUnsealed( size_t &size, uint16_t instanceId, uint32_t requestId, std::unordered_set<Key> &keys, std::unordered_set<Key>::iterator &it, uint32_t &keysCount, bool &isCompleted ) {
	// -- common/protocol/batch_protocol.cc --
	size = this->generateBatchKeyHeader(
		PROTO_MAGIC_REQUEST,
		PROTO_MAGIC_TO_SERVER,
		PROTO_OPCODE_RECONSTRUCTION_UNSEALED,
		instanceId, requestId,
		keys, it, keysCount,
		isCompleted
	);
	return this->buffer.send;
}

char *CoordinatorProtocol::ackCompletedReconstruction( size_t &size, uint16_t instanceId, uint32_t requestId, bool success ) {
	size = this->generateHeader(
		success ? PROTO_MAGIC_RESPONSE_SUCCESS : PROTO_MAGIC_RESPONSE_FAILURE,
		PROTO_MAGIC_TO_SERVER,
		PROTO_OPCODE_RECONSTRUCTION,
		0, // length
		instanceId, requestId
	);
	return this->buffer.send;
}

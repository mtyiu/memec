#include "protocol.hh"

char *ServerProtocol::resServerReconstructedMsg( size_t &size, uint16_t instanceId, uint32_t requestId ) {
	// -- common/protocol/recovery_protocol.cc --
	size = this->generateHeader(
		PROTO_MAGIC_RESPONSE_SUCCESS,
		PROTO_MAGIC_TO_COORDINATOR,
		PROTO_OPCODE_SERVER_RECONSTRUCTED,
		0, // length
		instanceId, requestId
	);
	return this->buffer.send;
}

char *ServerProtocol::resReconstruction( size_t &size, uint16_t instanceId, uint32_t requestId, uint32_t listId, uint32_t chunkId, uint32_t numStripes ) {
	// -- common/protocol/recovery_protocol.cc --
	size = this->generateReconstructionHeader(
		PROTO_MAGIC_RESPONSE_SUCCESS,
		PROTO_MAGIC_TO_COORDINATOR,
		PROTO_OPCODE_RECONSTRUCTION,
		instanceId, requestId,
		listId, chunkId, numStripes
	);
	return this->buffer.send;
}

char *ServerProtocol::resReconstructionUnsealed( size_t &size, uint16_t instanceId, uint32_t requestId, uint32_t listId, uint32_t chunkId, uint32_t numUnsealedKeys ) {
	// -- common/protocol/recovery_protocol.cc --
	size = this->generateReconstructionHeader(
		PROTO_MAGIC_RESPONSE_SUCCESS,
		PROTO_MAGIC_TO_COORDINATOR,
		PROTO_OPCODE_RECONSTRUCTION_UNSEALED,
		instanceId, requestId,
		listId, chunkId, numUnsealedKeys
	);
	return this->buffer.send;
}

char *ServerProtocol::resPromoteBackupServer( size_t &size, uint16_t instanceId, uint32_t requestId, uint32_t addr, uint16_t port, uint32_t numStripes, uint32_t numUnsealedKeys ) {
	// -- common/protocol/recovery_protocol.cc --
	size = this->generatePromoteBackupServerHeader(
		PROTO_MAGIC_RESPONSE_SUCCESS,
		PROTO_MAGIC_TO_COORDINATOR,
		PROTO_OPCODE_BACKUP_SERVER_PROMOTED,
		instanceId, requestId,
		addr, port, numStripes, numUnsealedKeys
	);
	return this->buffer.send;
}

char *ServerProtocol::reqBatchGetChunks(
	size_t &size, uint16_t instanceId, uint32_t requestId,
	std::vector<uint32_t> *requestIds,
	std::vector<Metadata> *metadata,
	uint32_t &chunksCount,
	bool &isCompleted
) {
	size = this->generateBatchChunkHeader(
		PROTO_MAGIC_REQUEST,
		PROTO_MAGIC_TO_SERVER,
		PROTO_OPCODE_BATCH_CHUNKS,
		instanceId, requestId,
		requestIds,
		metadata,
		chunksCount,
		isCompleted
	);
	return this->buffer.send;
}

char *ServerProtocol::sendUnsealedKeys(
	size_t &size, uint16_t instanceId, uint32_t requestId,
	std::unordered_set<Key> &keys, std::unordered_set<Key>::iterator &it,
	std::unordered_map<Key, KeyValue> *values, LOCK_T *lock,
	uint32_t &keyValuesCount,
	bool &isCompleted
) {
	// -- common/protocol/batch_protocol.cc --
	size = this->generateBatchKeyValueHeader(
		PROTO_MAGIC_REQUEST,
		PROTO_MAGIC_TO_SERVER,
		PROTO_OPCODE_BATCH_KEY_VALUES,
		instanceId, requestId,
		keys, it, values, lock, keyValuesCount,
		isCompleted
	);
	return this->buffer.send;
}

char *ServerProtocol::resUnsealedKeys(
	size_t &size, uint16_t instanceId, uint32_t requestId, bool success,
	struct BatchKeyValueHeader &header
) {
	size = this->generateBatchKeyHeader(
		success ? PROTO_MAGIC_RESPONSE_SUCCESS : PROTO_MAGIC_RESPONSE_FAILURE,
		PROTO_MAGIC_TO_SERVER,
		PROTO_OPCODE_BATCH_KEY_VALUES,
		instanceId, requestId,
		header
	);
	return this->buffer.send;
}

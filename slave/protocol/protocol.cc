#include "protocol.hh"

char *SlaveProtocol::reqRegisterCoordinator( size_t &size ) {
	size = this->generateHeader(
		PROTO_MAGIC_REQUEST,
		PROTO_MAGIC_TO_COORDINATOR,
		PROTO_OPCODE_REGISTER,
		0
	);
	return this->buffer.send;
}

char *SlaveProtocol::sendHeartbeat( size_t &size, struct HeartbeatHeader &header, std::map<Key, OpMetadata> &opMetadataMap, size_t &count ) {
	size = this->generateHeartbeatMessage(
		PROTO_MAGIC_HEARTBEAT,
		PROTO_MAGIC_TO_COORDINATOR,
		PROTO_OPCODE_SYNC,
		header,
		opMetadataMap,
		count
	);
	return this->buffer.send;
}

char *SlaveProtocol::resRegisterMaster( size_t &size, bool success ) {
	size = this->generateHeader(
		success ? PROTO_MAGIC_RESPONSE_SUCCESS : PROTO_MAGIC_RESPONSE_FAILURE,
		PROTO_MAGIC_TO_MASTER,
		PROTO_OPCODE_REGISTER,
		0
	);
	return this->buffer.send;
}

char *SlaveProtocol::resSet( size_t &size, bool success, uint8_t keySize, char *key ) {
	size = this->generateKeyHeader(
		success ? PROTO_MAGIC_RESPONSE_SUCCESS : PROTO_MAGIC_RESPONSE_FAILURE,
		PROTO_MAGIC_TO_MASTER,
		PROTO_OPCODE_SET,
		keySize,
		key
	);
	return this->buffer.send;
}

char *SlaveProtocol::resGet( size_t &size, bool success, uint8_t keySize, char *key, uint32_t valueSize, char *value ) {
	if ( success ) {
		size = this->generateKeyValueHeader(
			PROTO_MAGIC_RESPONSE_SUCCESS,
			PROTO_MAGIC_TO_MASTER,
			PROTO_OPCODE_GET,
			keySize,
			key,
			valueSize,
			value
		);
	} else {
		size = this->generateKeyHeader(
			PROTO_MAGIC_RESPONSE_FAILURE,
			PROTO_MAGIC_TO_MASTER,
			PROTO_OPCODE_GET,
			keySize,
			key
		);
	}
	return this->buffer.send;
}

char *SlaveProtocol::resUpdate( size_t &size, uint8_t keySize, char *key, uint32_t listId, uint32_t stripeId, uint32_t chunkId, uint32_t offset, uint32_t length, char *delta ) {
	size = this->generateKeyChunkUpdateHeader(
		PROTO_MAGIC_RESPONSE_SUCCESS,
		PROTO_MAGIC_TO_MASTER,
		PROTO_OPCODE_UPDATE,
		keySize, key,
		listId, stripeId, chunkId,
		offset, length, delta
	);
	return this->buffer.send;
}

char *SlaveProtocol::resUpdate( size_t &size, uint8_t keySize, char *key ) {
	size = this->generateKeyHeader(
		PROTO_MAGIC_RESPONSE_FAILURE,
		PROTO_MAGIC_TO_MASTER,
		PROTO_OPCODE_UPDATE,
		keySize,
		key
	);
	return this->buffer.send;
}

char *SlaveProtocol::resUpdateChunk( size_t &size, bool success, uint32_t listId, uint32_t stripeId, uint32_t chunkId, uint32_t offset, uint32_t length ) {
	size = this->generateChunkUpdateHeader(
		success ? PROTO_MAGIC_RESPONSE_SUCCESS : PROTO_MAGIC_RESPONSE_FAILURE,
		PROTO_MAGIC_TO_MASTER,
		PROTO_OPCODE_UPDATE_CHUNK,
		listId, stripeId, chunkId,
		offset, length
	);
	return this->buffer.send;
}

char *SlaveProtocol::resDelete( size_t &size, uint8_t keySize, char *key, uint32_t listId, uint32_t stripeId, uint32_t chunkId, uint32_t offset, uint32_t length, char *delta ) {
	size = this->generateKeyChunkUpdateHeader(
		PROTO_MAGIC_RESPONSE_SUCCESS,
		PROTO_MAGIC_TO_MASTER,
		PROTO_OPCODE_DELETE,
		keySize, key,
		listId, stripeId, chunkId,
		offset, length, delta
	);
	return this->buffer.send;
}

char *SlaveProtocol::resDelete( size_t &size, uint8_t keySize, char *key ) {
	size = this->generateKeyHeader(
		PROTO_MAGIC_RESPONSE_FAILURE,
		PROTO_MAGIC_TO_MASTER,
		PROTO_OPCODE_DELETE,
		keySize,
		key
	);
	return this->buffer.send;
}

char *SlaveProtocol::resDeleteChunk( size_t &size, bool success, uint32_t listId, uint32_t stripeId, uint32_t chunkId, uint32_t offset, uint32_t length ) {
	size = this->generateChunkUpdateHeader(
		success ? PROTO_MAGIC_RESPONSE_SUCCESS : PROTO_MAGIC_RESPONSE_FAILURE,
		PROTO_MAGIC_TO_MASTER,
		PROTO_OPCODE_DELETE_CHUNK,
		listId, stripeId, chunkId,
		offset, length
	);
	return this->buffer.send;
}

char *SlaveProtocol::reqRegisterSlavePeer( size_t &size ) {
	size = this->generateHeader(
		PROTO_MAGIC_REQUEST,
		PROTO_MAGIC_TO_SLAVE,
		PROTO_OPCODE_REGISTER,
		0
	);
	return this->buffer.send;
}

char *SlaveProtocol::resRegisterSlavePeer( size_t &size, bool success ) {
	size = this->generateHeader(
		success ? PROTO_MAGIC_RESPONSE_SUCCESS : PROTO_MAGIC_RESPONSE_FAILURE,
		PROTO_MAGIC_TO_SLAVE,
		PROTO_OPCODE_REGISTER,
		0
	);
	return this->buffer.send;
}

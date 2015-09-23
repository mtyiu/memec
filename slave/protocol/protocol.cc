#include "protocol.hh"

bool SlaveProtocol::init( size_t size, uint32_t dataChunkCount ) {
	this->status = new bool[ dataChunkCount ];
	return Protocol::init( size );
}

void SlaveProtocol::free() {
	delete[] this->status;
	Protocol::free();
}

char *SlaveProtocol::reqRegisterCoordinator( size_t &size, uint32_t id, uint32_t addr, uint16_t port ) {
	size = this->generateAddressHeader(
		PROTO_MAGIC_REQUEST,
		PROTO_MAGIC_TO_COORDINATOR,
		PROTO_OPCODE_REGISTER,
		id,
		addr, port
	);
	return this->buffer.send;
}

char *SlaveProtocol::sendHeartbeat( size_t &size, uint32_t id, struct HeartbeatHeader &header, std::map<Key, OpMetadata> &opMetadataMap, size_t &count ) {
	size = this->generateHeartbeatMessage(
		PROTO_MAGIC_HEARTBEAT,
		PROTO_MAGIC_TO_COORDINATOR,
		PROTO_OPCODE_SYNC,
		id,
		header,
		opMetadataMap,
		count
	);
	return this->buffer.send;
}

char *SlaveProtocol::resRegisterMaster( size_t &size, uint32_t id, bool success ) {
	size = this->generateHeader(
		success ? PROTO_MAGIC_RESPONSE_SUCCESS : PROTO_MAGIC_RESPONSE_FAILURE,
		PROTO_MAGIC_TO_MASTER,
		PROTO_OPCODE_REGISTER,
		0, // length
		id
	);
	return this->buffer.send;
}

char *SlaveProtocol::resSet( size_t &size, uint32_t id, bool success, uint8_t keySize, char *key ) {
	size = this->generateKeyHeader(
		success ? PROTO_MAGIC_RESPONSE_SUCCESS : PROTO_MAGIC_RESPONSE_FAILURE,
		PROTO_MAGIC_TO_MASTER,
		PROTO_OPCODE_SET,
		id,
		keySize,
		key
	);
	return this->buffer.send;
}

char *SlaveProtocol::resRemappingSetLock( size_t &size, uint32_t id, bool success, uint32_t listId, uint32_t chunkId, uint8_t keySize, char *key ) {
	size = this->generateRemappingLockHeader(
		success ? PROTO_MAGIC_RESPONSE_SUCCESS : PROTO_MAGIC_RESPONSE_FAILURE,
		PROTO_MAGIC_TO_MASTER,
		PROTO_OPCODE_REMAPPING_LOCK,
		id,
		listId,
		chunkId,
		keySize,
		key
	);
	return this->buffer.send;
}

char *SlaveProtocol::resGet( size_t &size, uint32_t id, bool success, uint8_t keySize, char *key, uint32_t valueSize, char *value ) {
	if ( success ) {
		size = this->generateKeyValueHeader(
			PROTO_MAGIC_RESPONSE_SUCCESS,
			PROTO_MAGIC_TO_MASTER,
			PROTO_OPCODE_GET,
			id,
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
			id,
			keySize,
			key
		);
	}
	return this->buffer.send;
}

char *SlaveProtocol::resUpdate( size_t &size, uint32_t id, bool success, uint8_t keySize, char *key, uint32_t valueUpdateOffset, uint32_t valueUpdateSize ) {
	size = this->generateKeyValueUpdateHeader(
		success ? PROTO_MAGIC_RESPONSE_SUCCESS : PROTO_MAGIC_RESPONSE_FAILURE,
		PROTO_MAGIC_TO_MASTER,
		PROTO_OPCODE_UPDATE,
		id,
		keySize, key,
		valueUpdateOffset, valueUpdateSize, 0
	);
	return this->buffer.send;
}

char *SlaveProtocol::resDelete( size_t &size, uint32_t id, bool success, uint8_t keySize, char *key ) {
	size = this->generateKeyHeader(
		success ? PROTO_MAGIC_RESPONSE_SUCCESS : PROTO_MAGIC_RESPONSE_FAILURE,
		PROTO_MAGIC_TO_MASTER,
		PROTO_OPCODE_DELETE,
		id,
		keySize,
		key
	);
	return this->buffer.send;
}

char *SlaveProtocol::reqRegisterSlavePeer( size_t &size, uint32_t id, ServerAddr *addr ) {
	size = this->generateAddressHeader(
		PROTO_MAGIC_REQUEST,
		PROTO_MAGIC_TO_SLAVE,
		PROTO_OPCODE_REGISTER,
		id,
		addr->addr, addr->port
	);
	return this->buffer.send;
}

char *SlaveProtocol::resRegisterSlavePeer( size_t &size, uint32_t id, bool success ) {
	size = this->generateHeader(
		success ? PROTO_MAGIC_RESPONSE_SUCCESS : PROTO_MAGIC_RESPONSE_FAILURE,
		PROTO_MAGIC_TO_SLAVE,
		PROTO_OPCODE_REGISTER,
		0, // length
		id
	);
	return this->buffer.send;
}

char *SlaveProtocol::reqUpdateChunk( size_t &size, uint32_t id, uint32_t listId, uint32_t stripeId, uint32_t chunkId, uint32_t offset, uint32_t length, uint32_t updatingChunkId, char *delta, char *buf ) {
	if ( ! buf ) buf = this->buffer.send;
	size = this->generateChunkUpdateHeader(
		PROTO_MAGIC_REQUEST,
		PROTO_MAGIC_TO_SLAVE,
		PROTO_OPCODE_UPDATE_CHUNK,
		id,
		listId, stripeId, chunkId,
		offset, length, updatingChunkId,
		delta,
		buf
	);
	return buf;
}

char *SlaveProtocol::resUpdateChunk( size_t &size, uint32_t id, bool success, uint32_t listId, uint32_t stripeId, uint32_t chunkId, uint32_t offset, uint32_t length, uint32_t updatingChunkId ) {
	size = this->generateChunkUpdateHeader(
		success ? PROTO_MAGIC_RESPONSE_SUCCESS : PROTO_MAGIC_RESPONSE_FAILURE,
		PROTO_MAGIC_TO_SLAVE,
		PROTO_OPCODE_UPDATE_CHUNK,
		id,
		listId, stripeId, chunkId,
		offset, length, updatingChunkId
	);
	return this->buffer.send;
}

char *SlaveProtocol::reqDeleteChunk( size_t &size, uint32_t id, uint32_t listId, uint32_t stripeId, uint32_t chunkId, uint32_t offset, uint32_t length, uint32_t updatingChunkId, char *delta, char *buf ) {
	if ( ! buf ) buf = this->buffer.send;
	size = this->generateChunkUpdateHeader(
		PROTO_MAGIC_REQUEST,
		PROTO_MAGIC_TO_SLAVE,
		PROTO_OPCODE_DELETE_CHUNK,
		id,
		listId, stripeId, chunkId,
		offset, length, updatingChunkId,
		delta,
		buf
	);
	return this->buffer.send;
}

char *SlaveProtocol::resDeleteChunk( size_t &size, uint32_t id, bool success, uint32_t listId, uint32_t stripeId, uint32_t chunkId, uint32_t offset, uint32_t length, uint32_t updatingChunkId ) {
	size = this->generateChunkUpdateHeader(
		success ? PROTO_MAGIC_RESPONSE_SUCCESS : PROTO_MAGIC_RESPONSE_FAILURE,
		PROTO_MAGIC_TO_SLAVE,
		PROTO_OPCODE_DELETE_CHUNK,
		id,
		listId, stripeId, chunkId,
		offset, length, updatingChunkId
	);
	return this->buffer.send;
}

char *SlaveProtocol::reqGetChunk( size_t &size, uint32_t id, uint32_t listId, uint32_t stripeId, uint32_t chunkId ) {
	size = this->generateChunkHeader(
		PROTO_MAGIC_REQUEST,
		PROTO_MAGIC_TO_SLAVE,
		PROTO_OPCODE_GET_CHUNK,
		id,
		listId, stripeId, chunkId
	);
	return this->buffer.send;
}

char *SlaveProtocol::resGetChunk( size_t &size, uint32_t id, bool success, uint32_t listId, uint32_t stripeId, uint32_t chunkId, uint32_t chunkSize, char *chunkData ) {
	if ( success ) {
		size = this->generateChunkDataHeader(
			PROTO_MAGIC_RESPONSE_SUCCESS,
			PROTO_MAGIC_TO_SLAVE,
			PROTO_OPCODE_GET_CHUNK,
			id,
			listId, stripeId, chunkId,
			chunkSize, chunkData
		);
	} else {
		size = this->generateChunkHeader(
			PROTO_MAGIC_RESPONSE_FAILURE,
			PROTO_MAGIC_TO_SLAVE,
			PROTO_OPCODE_GET_CHUNK,
			id,
			listId, stripeId, chunkId
		);
	}
	return this->buffer.send;
}

char *SlaveProtocol::reqSetChunk( size_t &size, uint32_t id, uint32_t listId, uint32_t stripeId, uint32_t chunkId, uint32_t chunkSize, char *chunkData ) {
	size = this->generateChunkDataHeader(
		PROTO_MAGIC_REQUEST,
		PROTO_MAGIC_TO_SLAVE,
		PROTO_OPCODE_SET_CHUNK,
		id,
		listId, stripeId, chunkId,
		chunkSize, chunkData
	);
	return this->buffer.send;
}

char *SlaveProtocol::resSetChunk( size_t &size, uint32_t id, bool success, uint32_t listId, uint32_t stripeId, uint32_t chunkId ) {
	size = this->generateChunkHeader(
		success ? PROTO_MAGIC_RESPONSE_SUCCESS : PROTO_MAGIC_RESPONSE_FAILURE,
		PROTO_MAGIC_TO_SLAVE,
		PROTO_OPCODE_SET_CHUNK,
		id,
		listId, stripeId, chunkId
	);
	return this->buffer.send;
}

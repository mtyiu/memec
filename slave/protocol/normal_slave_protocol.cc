#include "protocol.hh"

char *SlaveProtocol::reqUpdate( size_t &size, uint16_t instanceId, uint32_t requestId, uint32_t listId, uint32_t stripeId, uint32_t chunkId, char *key, uint8_t keySize, char *valueUpdate, uint32_t valueUpdateOffset, uint32_t valueUpdateSize, uint32_t chunkUpdateOffset, char *buf, uint32_t timestamp ) {
	// -- common/protocol/normal_protocol.cc --
	if ( ! buf ) buf = this->buffer.send;
	size = this->generateChunkKeyValueUpdateHeader(
		PROTO_MAGIC_REQUEST,
		PROTO_MAGIC_TO_SLAVE,
		PROTO_OPCODE_UPDATE,
		instanceId, requestId,
		listId,
		stripeId,
		chunkId,
		keySize,
		key,
		valueUpdateOffset,
		valueUpdateSize,
		chunkUpdateOffset,
		valueUpdate,
		buf,
		timestamp
	);
	return buf;
}

char *SlaveProtocol::resUpdate( size_t &size, uint16_t instanceId, uint32_t requestId, bool success, uint32_t listId, uint32_t stripeId, uint32_t chunkId, char *key, uint8_t keySize, uint32_t valueUpdateOffset, uint32_t valueUpdateSize, uint32_t chunkUpdateOffset, char *buf ) {
	// -- common/protocol/normal_protocol.cc --
	if ( ! buf ) buf = this->buffer.send;
	size = this->generateChunkKeyValueUpdateHeader(
		success ? PROTO_MAGIC_RESPONSE_SUCCESS : PROTO_MAGIC_RESPONSE_FAILURE,
		PROTO_MAGIC_TO_SLAVE,
		PROTO_OPCODE_UPDATE,
		instanceId, requestId,
		listId,
		stripeId,
		chunkId,
		keySize,
		key,
		valueUpdateOffset,
		valueUpdateSize,
		chunkUpdateOffset,
		buf
	);
	return buf;
}

char *SlaveProtocol::reqDelete( size_t &size, uint16_t instanceId, uint32_t requestId, uint32_t listId, uint32_t stripeId, uint32_t chunkId, char *key, uint8_t keySize, char *buf, uint32_t timestamp ) {
	// -- common/protocol/normal_protocol.cc --
	if ( ! buf ) buf = this->buffer.send;
	size = this->generateChunkKeyHeader(
		PROTO_MAGIC_REQUEST,
		PROTO_MAGIC_TO_SLAVE,
		PROTO_OPCODE_DELETE,
		instanceId, requestId,
		listId,
		stripeId,
		chunkId,
		keySize,
		key,
		buf,
		timestamp
	);
	return buf;
}

char *SlaveProtocol::resDelete( size_t &size, uint16_t instanceId, uint32_t requestId, bool success, uint32_t listId, uint32_t stripeId, uint32_t chunkId, char *key, uint8_t keySize, char *buf ) {
	// -- common/protocol/normal_protocol.cc --
	if ( ! buf ) buf = this->buffer.send;
	size = this->generateChunkKeyHeader(
		success ? PROTO_MAGIC_RESPONSE_SUCCESS : PROTO_MAGIC_RESPONSE_FAILURE,
		PROTO_MAGIC_TO_SLAVE,
		PROTO_OPCODE_DELETE,
		instanceId, requestId,
		listId,
		stripeId,
		chunkId,
		keySize,
		key,
		buf
	);
	return buf;
}

char *SlaveProtocol::reqGetChunk( size_t &size, uint16_t instanceId, uint32_t requestId, uint32_t listId, uint32_t stripeId, uint32_t chunkId, char *buf ) {
	// -- common/protocol/normal_protocol.cc --
	size = this->generateChunkHeader(
		PROTO_MAGIC_REQUEST,
		PROTO_MAGIC_TO_SLAVE,
		PROTO_OPCODE_GET_CHUNK,
		instanceId, requestId,
		listId, stripeId, chunkId,
		buf
	);
	return this->buffer.send;
}

char *SlaveProtocol::resGetChunk( size_t &size, uint16_t instanceId, uint32_t requestId, bool success, uint32_t listId, uint32_t stripeId, uint32_t chunkId, uint32_t chunkSize, uint32_t chunkOffset, char *chunkData ) {
	// -- common/protocol/normal_protocol.cc --
	if ( success ) {
		size = this->generateChunkDataHeader(
			PROTO_MAGIC_RESPONSE_SUCCESS,
			PROTO_MAGIC_TO_SLAVE,
			PROTO_OPCODE_GET_CHUNK,
			instanceId, requestId,
			listId, stripeId, chunkId,
			chunkSize, chunkOffset, chunkData
		);
	} else {
		size = this->generateChunkHeader(
			PROTO_MAGIC_RESPONSE_FAILURE,
			PROTO_MAGIC_TO_SLAVE,
			PROTO_OPCODE_GET_CHUNK,
			instanceId, requestId,
			listId, stripeId, chunkId
		);
	}
	return this->buffer.send;
}

char *SlaveProtocol::reqSetChunk( size_t &size, uint16_t instanceId, uint32_t requestId, uint32_t listId, uint32_t stripeId, uint32_t chunkId, uint32_t chunkSize, uint32_t chunkOffset, char *chunkData ) {
	// -- common/protocol/normal_protocol.cc --
	size = this->generateChunkDataHeader(
		PROTO_MAGIC_REQUEST,
		PROTO_MAGIC_TO_SLAVE,
		PROTO_OPCODE_SET_CHUNK,
		instanceId, requestId,
		listId, stripeId, chunkId,
		chunkSize, chunkOffset, chunkData
	);
	return this->buffer.send;
}

char *SlaveProtocol::reqSetChunk( size_t &size, uint16_t instanceId, uint32_t requestId, uint32_t listId, uint32_t stripeId, uint32_t chunkId, std::unordered_map<Key, KeyValue> *values, std::unordered_multimap<Metadata, Key> *metadataRev, std::unordered_set<Key> *deleted, LOCK_T *lock, bool &isCompleted ) {
	// -- common/protocol/normal_protocol.cc --
	size = this->generateChunkKeyValueHeader(
		PROTO_MAGIC_REQUEST,
		PROTO_MAGIC_TO_SLAVE,
		PROTO_OPCODE_SET_CHUNK_UNSEALED,
		instanceId, requestId,
		listId, stripeId, chunkId,
		values, metadataRev, deleted, lock,
		isCompleted
	);
	return this->buffer.send;
}

char *SlaveProtocol::resSetChunk( size_t &size, uint16_t instanceId, uint32_t requestId, bool success, uint32_t listId, uint32_t stripeId, uint32_t chunkId ) {
	// -- common/protocol/normal_protocol.cc --
	size = this->generateChunkHeader(
		success ? PROTO_MAGIC_RESPONSE_SUCCESS : PROTO_MAGIC_RESPONSE_FAILURE,
		PROTO_MAGIC_TO_SLAVE,
		PROTO_OPCODE_SET_CHUNK,
		instanceId, requestId,
		listId, stripeId, chunkId
	);
	return this->buffer.send;
}

char *SlaveProtocol::reqUpdateChunk( size_t &size, uint16_t instanceId, uint32_t requestId, uint32_t listId, uint32_t stripeId, uint32_t chunkId, uint32_t offset, uint32_t length, uint32_t updatingChunkId, char *delta, char *buf, uint32_t timestamp, bool checkGetChunk ) {
	// -- common/protocol/normal_protocol.cc --
	if ( ! buf ) buf = this->buffer.send;
	size = this->generateChunkUpdateHeader(
		PROTO_MAGIC_REQUEST,
		PROTO_MAGIC_TO_SLAVE,
		checkGetChunk ? PROTO_OPCODE_UPDATE_CHUNK_CHECK : PROTO_OPCODE_UPDATE_CHUNK,
		instanceId, requestId,
		listId, stripeId, chunkId,
		offset, length, updatingChunkId,
		delta,
		buf,
		timestamp
	);
	return buf;
}

char *SlaveProtocol::resUpdateChunk( size_t &size, uint16_t instanceId, uint32_t requestId, bool success, uint32_t listId, uint32_t stripeId, uint32_t chunkId, uint32_t offset, uint32_t length, uint32_t updatingChunkId ) {
	// -- common/protocol/normal_protocol.cc --
	size = this->generateChunkUpdateHeader(
		success ? PROTO_MAGIC_RESPONSE_SUCCESS : PROTO_MAGIC_RESPONSE_FAILURE,
		PROTO_MAGIC_TO_SLAVE,
		PROTO_OPCODE_UPDATE_CHUNK,
		instanceId, requestId,
		listId, stripeId, chunkId,
		offset, length, updatingChunkId
	);
	return this->buffer.send;
}

char *SlaveProtocol::reqDeleteChunk( size_t &size, uint16_t instanceId, uint32_t requestId, uint32_t listId, uint32_t stripeId, uint32_t chunkId, uint32_t offset, uint32_t length, uint32_t updatingChunkId, char *delta, char *buf, uint32_t timestamp, bool checkGetChunk ) {
	// -- common/protocol/normal_protocol.cc --
	if ( ! buf ) buf = this->buffer.send;
	size = this->generateChunkUpdateHeader(
		PROTO_MAGIC_REQUEST,
		PROTO_MAGIC_TO_SLAVE,
		checkGetChunk ? PROTO_OPCODE_DELETE_CHUNK_CHECK : PROTO_OPCODE_DELETE_CHUNK,
		instanceId, requestId,
		listId, stripeId, chunkId,
		offset, length, updatingChunkId,
		delta,
		buf,
		timestamp
	);
	return this->buffer.send;
}

char *SlaveProtocol::resDeleteChunk( size_t &size, uint16_t instanceId, uint32_t requestId, bool success, uint32_t listId, uint32_t stripeId, uint32_t chunkId, uint32_t offset, uint32_t length, uint32_t updatingChunkId ) {
	// -- common/protocol/normal_protocol.cc --
	size = this->generateChunkUpdateHeader(
		success ? PROTO_MAGIC_RESPONSE_SUCCESS : PROTO_MAGIC_RESPONSE_FAILURE,
		PROTO_MAGIC_TO_SLAVE,
		PROTO_OPCODE_DELETE_CHUNK,
		instanceId, requestId,
		listId, stripeId, chunkId,
		offset, length, updatingChunkId
	);
	return this->buffer.send;
}

#include "protocol.hh"

char *SlaveProtocol::resSet( size_t &size, uint32_t id, uint32_t listId, uint32_t stripeId, uint32_t chunkId, uint8_t keySize, char *key ) {
	// -- common/protocol/normal_protocol.cc --
	size = this->generateChunkKeyHeader(
		PROTO_MAGIC_RESPONSE_SUCCESS,
		PROTO_MAGIC_TO_MASTER,
		PROTO_OPCODE_SET,
		id,
		listId,
		stripeId,
		chunkId,
		keySize,
		key
	);
	return this->buffer.send;
}

char *SlaveProtocol::resSet( size_t &size, uint32_t id, bool success, uint8_t keySize, char *key, bool toMaster ) {
	// -- common/protocol/normal_protocol.cc --
	size = this->generateKeyHeader(
		success ? PROTO_MAGIC_RESPONSE_SUCCESS : PROTO_MAGIC_RESPONSE_FAILURE,
		toMaster ? PROTO_MAGIC_TO_MASTER : PROTO_MAGIC_TO_SLAVE,
		PROTO_OPCODE_SET,
		id,
		keySize,
		key
	);
	return this->buffer.send;
}

char *SlaveProtocol::resGet( size_t &size, uint32_t id, bool success, bool isDegraded, uint8_t keySize, char *key, uint32_t valueSize, char *value, bool toMaster ) {
	// -- common/protocol/normal_protocol.cc --
	if ( success ) {
		size = this->generateKeyValueHeader(
			PROTO_MAGIC_RESPONSE_SUCCESS,
			toMaster ? PROTO_MAGIC_TO_MASTER : PROTO_MAGIC_TO_SLAVE,
			isDegraded ? PROTO_OPCODE_DEGRADED_GET : PROTO_OPCODE_GET,
			id,
			keySize,
			key,
			valueSize,
			value
		);
	} else {
		size = this->generateKeyHeader(
			PROTO_MAGIC_RESPONSE_FAILURE,
			toMaster ? PROTO_MAGIC_TO_MASTER : PROTO_MAGIC_TO_SLAVE,
			isDegraded ? PROTO_OPCODE_DEGRADED_GET : PROTO_OPCODE_GET,
			id,
			keySize,
			key
		);
	}
	return this->buffer.send;
}

char *SlaveProtocol::resUpdate( size_t &size, uint32_t id, bool success, bool isDegraded, uint8_t keySize, char *key, uint32_t valueUpdateOffset, uint32_t valueUpdateSize ) {
	// -- common/protocol/normal_protocol.cc --
	size = this->generateKeyValueUpdateHeader(
		success ? PROTO_MAGIC_RESPONSE_SUCCESS : PROTO_MAGIC_RESPONSE_FAILURE,
		PROTO_MAGIC_TO_MASTER,
		isDegraded ? PROTO_OPCODE_DEGRADED_UPDATE : PROTO_OPCODE_UPDATE,
		id,
		keySize, key,
		valueUpdateOffset, valueUpdateSize, 0
	);
	return this->buffer.send;
}

char *SlaveProtocol::resDelete( size_t &size, uint32_t id, bool success, bool isDegraded, uint8_t keySize, char *key, bool toMaster ) {
	// -- common/protocol/normal_protocol.cc --
	size = this->generateKeyHeader(
		success ? PROTO_MAGIC_RESPONSE_SUCCESS : PROTO_MAGIC_RESPONSE_FAILURE,
		toMaster ? PROTO_MAGIC_TO_MASTER : PROTO_MAGIC_TO_SLAVE,
		isDegraded ? PROTO_OPCODE_DEGRADED_DELETE : PROTO_OPCODE_DELETE,
		id,
		keySize,
		key
	);
	return this->buffer.send;
}

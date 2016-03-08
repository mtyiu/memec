#include "protocol.hh"

char *ServerProtocol::resSet( size_t &size, uint16_t instanceId, uint32_t requestId, uint32_t timestamp, uint32_t listId, uint32_t stripeId, uint32_t chunkId, bool isSealed, uint32_t sealedListId, uint32_t sealedStripeId, uint32_t sealedChunkId, uint8_t keySize, char *key ) {
	// -- common/protocol/normal_protocol.cc --
	if ( isSealed ) {
		size = this->generateKeyBackupHeader(
			PROTO_MAGIC_RESPONSE_SUCCESS,
			PROTO_MAGIC_TO_CLIENT,
			PROTO_OPCODE_SET,
			instanceId, requestId,
			timestamp,
			listId,
			stripeId,
			chunkId,
			sealedListId,
			sealedStripeId,
			sealedChunkId,
			keySize,
			key
		);
	} else {
		size = this->generateKeyBackupHeader(
			PROTO_MAGIC_RESPONSE_SUCCESS,
			PROTO_MAGIC_TO_CLIENT,
			PROTO_OPCODE_SET,
			instanceId, requestId,
			timestamp,
			listId,
			stripeId,
			chunkId,
			keySize,
			key
		);
	}
	return this->buffer.send;
}

char *ServerProtocol::resSet( size_t &size, uint16_t instanceId, uint32_t requestId, bool success, uint8_t keySize, char *key, bool toMaster ) {
	// -- common/protocol/normal_protocol.cc --
	size = this->generateKeyBackupHeader(
		success ? PROTO_MAGIC_RESPONSE_SUCCESS : PROTO_MAGIC_RESPONSE_FAILURE,
		toMaster ? PROTO_MAGIC_TO_CLIENT : PROTO_MAGIC_TO_SERVER,
		PROTO_OPCODE_SET,
		instanceId, requestId,
		keySize,
		key
	);
	return this->buffer.send;
}

char *ServerProtocol::resGet( size_t &size, uint16_t instanceId, uint32_t requestId, bool success, bool isDegraded, uint8_t keySize, char *key, uint32_t valueSize, char *value, bool toMaster ) {
	// -- common/protocol/normal_protocol.cc --
	if ( success ) {
		size = this->generateKeyValueHeader(
			PROTO_MAGIC_RESPONSE_SUCCESS,
			toMaster ? PROTO_MAGIC_TO_CLIENT : PROTO_MAGIC_TO_SERVER,
			isDegraded ? PROTO_OPCODE_DEGRADED_GET : PROTO_OPCODE_GET,
			instanceId, requestId,
			keySize,
			key,
			valueSize,
			value
		);
	} else {
		size = this->generateKeyHeader(
			PROTO_MAGIC_RESPONSE_FAILURE,
			toMaster ? PROTO_MAGIC_TO_CLIENT : PROTO_MAGIC_TO_SERVER,
			isDegraded ? PROTO_OPCODE_DEGRADED_GET : PROTO_OPCODE_GET,
			instanceId, requestId,
			keySize,
			key
		);
	}
	return this->buffer.send;
}

char *ServerProtocol::resUpdate( size_t &size, uint16_t instanceId, uint32_t requestId, bool success, bool isDegraded, uint8_t keySize, char *key, uint32_t valueUpdateOffset, uint32_t valueUpdateSize ) {
	// -- common/protocol/normal_protocol.cc --
	size = this->generateKeyValueUpdateHeader(
		success ? PROTO_MAGIC_RESPONSE_SUCCESS : PROTO_MAGIC_RESPONSE_FAILURE,
		PROTO_MAGIC_TO_CLIENT,
		isDegraded ? PROTO_OPCODE_DEGRADED_UPDATE : PROTO_OPCODE_UPDATE,
		instanceId, requestId,
		keySize, key,
		valueUpdateOffset, valueUpdateSize, 0
	);
	return this->buffer.send;
}

char *ServerProtocol::resDelete( size_t &size, uint16_t instanceId, uint32_t requestId, bool isDegraded, uint32_t timestamp, uint32_t listId, uint32_t stripeId, uint32_t chunkId, uint8_t keySize, char *key, bool toMaster ) {
	// -- common/protocol/normal_protocol.cc --
	size = this->generateKeyBackupHeader(
		PROTO_MAGIC_RESPONSE_SUCCESS,
		toMaster ? PROTO_MAGIC_TO_CLIENT : PROTO_MAGIC_TO_SERVER,
		isDegraded ? PROTO_OPCODE_DEGRADED_DELETE : PROTO_OPCODE_DELETE,
		instanceId, requestId,
		timestamp,
		listId,
		stripeId,
		chunkId,
		keySize,
		key
	);
	return this->buffer.send;
}

char *ServerProtocol::resDelete( size_t &size, uint16_t instanceId, uint32_t requestId, bool isDegraded, uint8_t keySize, char *key, bool toMaster ) {
	// -- common/protocol/normal_protocol.cc --
	size = this->generateKeyBackupHeader(
		PROTO_MAGIC_RESPONSE_FAILURE,
		toMaster ? PROTO_MAGIC_TO_CLIENT : PROTO_MAGIC_TO_SERVER,
		isDegraded ? PROTO_OPCODE_DEGRADED_DELETE : PROTO_OPCODE_DELETE,
		instanceId, requestId,
		keySize,
		key
	);
	return this->buffer.send;
}

#include "protocol.hh"

char *MasterProtocol::reqSet( size_t &size, uint16_t instanceId, uint32_t requestId, char *key, uint8_t keySize, char *value, uint32_t valueSize, char *buf ) {
	// -- common/protocol/normal_protocol.cc --
	if ( ! buf ) buf = this->buffer.send;
	size = this->generateKeyValueHeader(
		PROTO_MAGIC_REQUEST,
		PROTO_MAGIC_TO_SLAVE,
		PROTO_OPCODE_SET,
		instanceId, requestId,
		keySize,
		key,
		valueSize,
		value,
		buf
	);
	return buf;
}

char *MasterProtocol::reqGet( size_t &size, uint16_t instanceId, uint32_t requestId, char *key, uint8_t keySize ) {
	// -- common/protocol/normal_protocol.cc --
	size = this->generateKeyHeader(
		PROTO_MAGIC_REQUEST,
		PROTO_MAGIC_TO_SLAVE,
		PROTO_OPCODE_GET,
		instanceId, requestId,
		keySize,
		key
	);
	return this->buffer.send;
}

char *MasterProtocol::reqUpdate( size_t &size, uint16_t instanceId, uint32_t requestId, char *key, uint8_t keySize, char *valueUpdate, uint32_t valueUpdateOffset, uint32_t valueUpdateSize, uint32_t timestamp ) {
	// -- common/protocol/normal_protocol.cc --
	size = this->generateKeyValueUpdateHeader(
		PROTO_MAGIC_REQUEST,
		PROTO_MAGIC_TO_SLAVE,
		PROTO_OPCODE_UPDATE,
		instanceId, requestId,
		keySize,
		key,
		valueUpdateOffset,
		valueUpdateSize,
		valueUpdate,
		0,
		timestamp
	);
	return this->buffer.send;
}

char *MasterProtocol::reqDelete( size_t &size, uint16_t instanceId, uint32_t requestId, char *key, uint8_t keySize, uint32_t timestamp ) {
	// -- common/protocol/normal_protocol.cc --
	size = this->generateKeyHeader(
		PROTO_MAGIC_REQUEST,
		PROTO_MAGIC_TO_SLAVE,
		PROTO_OPCODE_DELETE,
		instanceId, requestId,
		keySize,
		key, 0,
		timestamp
	);
	return this->buffer.send;
}

char *MasterProtocol::resSet( size_t &size, uint16_t instanceId, uint32_t requestId, bool success, uint8_t keySize, char *key ) {
	// -- common/protocol/normal_protocol.cc --
	success = true;
	size = this->generateKeyHeader(
		success ? PROTO_MAGIC_RESPONSE_SUCCESS : PROTO_MAGIC_RESPONSE_FAILURE,
		PROTO_MAGIC_TO_APPLICATION,
		PROTO_OPCODE_SET,
		instanceId, requestId,
		keySize,
		key
	);
	return this->buffer.send;
}

char *MasterProtocol::resGet( size_t &size, uint16_t instanceId, uint32_t requestId, bool success, uint8_t keySize, char *key, uint32_t valueSize, char *value ) {
	// -- common/protocol/normal_protocol.cc --
	if ( success ) {
		size = this->generateKeyValueHeader(
			PROTO_MAGIC_RESPONSE_SUCCESS,
			PROTO_MAGIC_TO_APPLICATION,
			PROTO_OPCODE_GET,
			instanceId, requestId,
			keySize,
			key,
			valueSize,
			value
		);
	} else {
		size = this->generateKeyHeader(
			PROTO_MAGIC_RESPONSE_FAILURE,
			PROTO_MAGIC_TO_APPLICATION,
			PROTO_OPCODE_GET,
			instanceId, requestId,
			keySize,
			key
		);
	}
	return this->buffer.send;
}

char *MasterProtocol::resUpdate( size_t &size, uint16_t instanceId, uint32_t requestId, bool success, uint8_t keySize, char *key, uint32_t valueUpdateOffset, uint32_t valueUpdateSize ) {
	// -- common/protocol/normal_protocol.cc --
	size = this->generateKeyValueUpdateHeader(
		success ? PROTO_MAGIC_RESPONSE_SUCCESS : PROTO_MAGIC_RESPONSE_FAILURE,
		PROTO_MAGIC_TO_APPLICATION,
		PROTO_OPCODE_UPDATE,
		instanceId, requestId,
		keySize, key,
		valueUpdateOffset, valueUpdateSize, 0
	);
	return this->buffer.send;
}

char *MasterProtocol::resDelete( size_t &size, uint16_t instanceId, uint32_t requestId, bool success, uint8_t keySize, char *key ) {
	// -- common/protocol/normal_protocol.cc --
	size = this->generateKeyHeader(
		success ? PROTO_MAGIC_RESPONSE_SUCCESS : PROTO_MAGIC_RESPONSE_FAILURE,
		PROTO_MAGIC_TO_APPLICATION,
		PROTO_OPCODE_DELETE,
		instanceId, requestId,
		keySize,
		key
	);
	return this->buffer.send;
}

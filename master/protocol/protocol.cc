#include "protocol.hh"

bool MasterProtocol::init( size_t size, uint32_t parityChunkCount ) {
	this->status = new bool[ parityChunkCount ];
	return Protocol::init( size );
}

void MasterProtocol::free() {
	delete[] this->status;
	Protocol::free();
}

char *MasterProtocol::reqRegisterCoordinator( size_t &size, uint32_t addr, uint16_t port ) {
	size = this->generateAddressHeader(
		PROTO_MAGIC_REQUEST,
		PROTO_MAGIC_TO_COORDINATOR,
		PROTO_OPCODE_REGISTER,
		addr, port
	);
	return this->buffer.send;
}

char *MasterProtocol::reqRegisterSlave( size_t &size, uint32_t addr, uint16_t port ) {
	size = this->generateAddressHeader(
		PROTO_MAGIC_REQUEST,
		PROTO_MAGIC_TO_SLAVE,
		PROTO_OPCODE_REGISTER,
		addr, port
	);
	return this->buffer.send;
}

char *MasterProtocol::reqSet( size_t &size, char *key, uint8_t keySize, char *value, uint32_t valueSize ) {
	size = this->generateKeyValueHeader(
		PROTO_MAGIC_REQUEST,
		PROTO_MAGIC_TO_SLAVE,
		PROTO_OPCODE_SET,
		keySize,
		key,
		valueSize,
		value
	);
	return this->buffer.send;
}

char *MasterProtocol::reqGet( size_t &size, char *key, uint8_t keySize ) {
	size = this->generateKeyHeader(
		PROTO_MAGIC_REQUEST,
		PROTO_MAGIC_TO_SLAVE,
		PROTO_OPCODE_GET,
		keySize,
		key
	);
	return this->buffer.send;
}

char *MasterProtocol::reqUpdate( size_t &size, char *key, uint8_t keySize, char *valueUpdate, uint32_t valueUpdateOffset, uint32_t valueUpdateSize ) {
	size = this->generateKeyValueUpdateHeader(
		PROTO_MAGIC_REQUEST,
		PROTO_MAGIC_TO_SLAVE,
		PROTO_OPCODE_UPDATE,
		keySize,
		key,
		valueUpdateOffset,
		valueUpdateSize,
		valueUpdate
	);
	return this->buffer.send;
}

char *MasterProtocol::reqDelete( size_t &size, char *key, uint8_t keySize ) {
	size = this->generateKeyHeader(
		PROTO_MAGIC_REQUEST,
		PROTO_MAGIC_TO_SLAVE,
		PROTO_OPCODE_DELETE,
		keySize,
		key
	);
	return this->buffer.send;
}

char *MasterProtocol::resRegisterApplication( size_t &size, bool success ) {
	size = this->generateHeader(
		success ? PROTO_MAGIC_RESPONSE_SUCCESS : PROTO_MAGIC_RESPONSE_FAILURE,
		PROTO_MAGIC_TO_APPLICATION,
		PROTO_OPCODE_REGISTER,
		0
	);
	return this->buffer.send;
}

char *MasterProtocol::resSet( size_t &size, bool success, uint8_t keySize, char *key ) {
	size = this->generateKeyHeader(
		success ? PROTO_MAGIC_RESPONSE_SUCCESS : PROTO_MAGIC_RESPONSE_FAILURE,
		PROTO_MAGIC_TO_APPLICATION,
		PROTO_OPCODE_SET,
		keySize,
		key
	);
	return this->buffer.send;
}

char *MasterProtocol::resGet( size_t &size, bool success, uint8_t keySize, char *key, uint32_t valueSize, char *value ) {
	if ( success ) {
		size = this->generateKeyValueHeader(
			PROTO_MAGIC_RESPONSE_SUCCESS,
			PROTO_MAGIC_TO_APPLICATION,
			PROTO_OPCODE_GET,
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
			keySize,
			key
		);
	}
	return this->buffer.send;
}

char *MasterProtocol::resUpdate( size_t &size, bool success, uint8_t keySize, char *key, uint32_t valueUpdateOffset, uint32_t valueUpdateSize ) {
	size = this->generateKeyValueUpdateHeader(
		success ? PROTO_MAGIC_RESPONSE_SUCCESS : PROTO_MAGIC_RESPONSE_FAILURE,
		PROTO_MAGIC_TO_APPLICATION,
		PROTO_OPCODE_UPDATE,
		keySize, key,
		valueUpdateOffset, valueUpdateSize, 0
	);
	return this->buffer.send;
}

char *MasterProtocol::resDelete( size_t &size, bool success, uint8_t keySize, char *key ) {
	size = this->generateKeyHeader(
		success ? PROTO_MAGIC_RESPONSE_SUCCESS : PROTO_MAGIC_RESPONSE_FAILURE,
		PROTO_MAGIC_TO_APPLICATION,
		PROTO_OPCODE_DELETE,
		keySize,
		key
	);
	return this->buffer.send;
}

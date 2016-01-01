#ifndef __BENCHMARK_HUAWEI_PROTOCOL_HH__
#define __BENCHMARK_HUAWEI_PROTOCOL_HH__

#include <stdint.h>
#include <arpa/inet.h>

/************************
 *  Magic byte (1 byte) *
 ************************/
// (Bit: 0-2) //
#define PROTO_MAGIC_REQUEST                       0x01 // -----001
#define PROTO_MAGIC_RESPONSE_SUCCESS              0x02 // -----010
#define PROTO_MAGIC_RESPONSE_FAILURE              0x03 // -----011
// (Bit: 3-4) //
#define PROTO_MAGIC_FROM_APPLICATION              0x00 // ---00---
#define PROTO_MAGIC_FROM_MASTER                   0x10 // ---10---
 // (Bit: 5-6) //
#define PROTO_MAGIC_TO_APPLICATION                0x00 // -00-----
#define PROTO_MAGIC_TO_MASTER                     0x40 // -10-----
// (Bit: 7): Reserved //

/*******************
 * Opcode (1 byte) *
 *******************/
#define PROTO_OPCODE_REGISTER                     0x00
#define PROTO_OPCODE_GET                          0x01
#define PROTO_OPCODE_SET                          0x02
#define PROTO_OPCODE_UPDATE                       0x03
#define PROTO_OPCODE_DELETE                       0x04

/*********************
 * Key size (1 byte) *
 *********************/
#define MAXIMUM_KEY_SIZE 255
/***********************
 * Value size (3 byte) *
 ***********************/
#define MAXIMUM_VALUE_SIZE 16777215

#define PROTO_HEADER_SIZE 16
struct ProtocolHeader {
	uint8_t magic, from, to, opcode;
	uint32_t length; // Content length
	uint16_t instanceId;
	uint32_t requestId;
	uint32_t timestamp;
};

 ///////////////////////
 // Normal operations //
 ///////////////////////
#define PROTO_KEY_SIZE 1
struct KeyHeader {
	uint8_t keySize;
	char *key;
};

#define PROTO_KEY_VALUE_SIZE 4
struct KeyValueHeader {
	uint8_t keySize;
	uint32_t valueSize; // 3 bytes
	char *key;
	char *value;
};

#define PROTO_KEY_VALUE_UPDATE_SIZE 7
struct KeyValueUpdateHeader {
	uint8_t keySize;
	uint32_t valueUpdateSize;   // 3 bytes
	uint32_t valueUpdateOffset; // 3 bytes
	char *key;
	char *valueUpdate;
}; // UPDATE request and UPDATE (fail) response

#define PROTO_BUF_MIN_SIZE		65536

class Protocol {
protected:
	bool parseHeader(
		uint8_t &magic, uint8_t &from, uint8_t &to, uint8_t &opcode,
		uint32_t &length, uint16_t &instanceId, uint32_t &requestId,
		uint32_t &requestTimestamp, char *buf, size_t size
	);
	bool parseKeyHeader(
		size_t offset,
		uint8_t &keySize, char *&key,
		char *buf, size_t size
	);
	bool parseKeyValueHeader(
		size_t offset, uint8_t &keySize, char *&key,
		uint32_t &valueSize, char *&value,
		char *buf, size_t size
	);
	bool parseKeyValueUpdateHeader(
		size_t offset, uint8_t &keySize, char *&key,
		uint32_t &valueUpdateOffset, uint32_t &valueUpdateSize,
		char *buf, size_t size
	);

public:
	size_t generateHeader(
		uint8_t magic, uint8_t opcode,
		uint32_t length, uint16_t instanceId, uint32_t requestId,
		char *buf, uint32_t requestTimestamp = 0
	);
	size_t generateKeyHeader(
		uint8_t magic, uint8_t opcode, uint16_t instanceId, uint32_t requestId,
		uint8_t keySize, char *key,
		char *buf
	);
	size_t generateKeyValueHeader(
		uint8_t magic, uint8_t opcode, uint16_t instanceId, uint32_t requestId,
		uint8_t keySize, char *key, uint32_t valueSize, char *value,
		char *buf
	);
	size_t generateKeyValueUpdateHeader(
		uint8_t magic, uint8_t opcode, uint16_t instanceId, uint32_t requestId,
		uint8_t keySize, char *key,
		uint32_t valueUpdateOffset, uint32_t valueUpdateSize, char *valueUpdate,
		char *buf
	);

	bool parseHeader(
		struct ProtocolHeader &header,
		char *buf = 0, size_t size = 0
	);
	bool parseKeyHeader(
		struct KeyHeader &header,
		char *buf = 0, size_t size = 0, size_t offset = 0
	);
	bool parseKeyValueHeader(
		struct KeyValueHeader &header,
		char *buf = 0, size_t size = 0, size_t offset = 0
	);
	bool parseKeyValueUpdateHeader(
		struct KeyValueUpdateHeader &header,
		char *buf = 0, size_t size = 0, size_t offset = 0
	);
};

#endif

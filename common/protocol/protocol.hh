#ifndef __COMMON_PROTOCOL_PROTOCOL_HH__
#define __COMMON_PROTOCOL_PROTOCOL_HH__

/**************************************
 * Packet format:
 * - Magic byte (1 byte)
 * - Opcode (1 byte)
 * - Reserved (2 bytes)
 * - Message length (4 bytes)
 ***************************************/

/************************
 *  Magic byte (1 byte) *
 ************************/
// (Bit: 0-2) //
#define PROTO_MAGIC_HEARTBEAT			0x00 // -----000
#define PROTO_MAGIC_REQUEST				0x01 // -----001
#define PROTO_MAGIC_RESPONSE_SUCCESS	0x02 // -----010
#define PROTO_MAGIC_RESPONSE_FAILURE	0x03 // -----011
#define PROTO_MAGIC_RESERVED_1			0x04 // -----100
#define PROTO_MAGIC_RESERVED_2			0x05 // -----101
#define PROTO_MAGIC_RESERVED_3			0x06 // -----110
#define PROTO_MAGIC_RESERVED_4			0x07 // -----111

// (Bit: 3-4) //
#define PROTO_MAGIC_FROM_APPLICATION	0x00 // ---00---
#define PROTO_MAGIC_FROM_COORDINATOR	0x08 // ---01---
#define PROTO_MAGIC_FROM_MASTER			0x10 // ---10---
#define PROTO_MAGIC_FROM_SLAVE			0x18 // ---11---
 // (Bit: 5-6) //
#define PROTO_MAGIC_TO_APPLICATION		0x00 // -00-----
#define PROTO_MAGIC_TO_COORDINATOR		0x50 // -01-----
#define PROTO_MAGIC_TO_MASTER			0x40 // -10-----
#define PROTO_MAGIC_TO_SLAVE			0x60 // -11-----
// (Bit: 7): Reserved //

/*******************
 * Opcode (1 byte) *
 *******************/
// Coordinator-specific opcodes //
#define PROTO_OPCODE_REGISTER			0x00
#define PROTO_OPCODE_GET_CONFIG			0x01

/****************
 * Internal use *
 ****************/
#define PROTO_HEADER_SIZE				8

#include <stdint.h>
#include <arpa/inet.h>

enum Role {
	ROLE_COORDINATOR,
	ROLE_MASTER,
	ROLE_SLAVE
};

class Protocol {
protected:
	uint8_t from, to;

	size_t generateHeader( uint8_t magic, uint8_t to, uint8_t opcode, uint32_t length );

public:
	struct {
		size_t size;
		char *data;
	} buffer;

	Protocol( Role role );
	bool init( size_t size = 0 );
	void free();
	bool parseHeader( char *buf, size_t size, uint8_t &magic, uint8_t &from, uint8_t &opcode, uint32_t &length );
	static size_t getSuggestedBufferSize( uint32_t keySize, uint32_t chunkSize );
};

#endif

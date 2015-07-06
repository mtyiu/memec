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
// (Bit: 0-3) //
#define PROTO_MAGIC_REQUEST				0x01
#define PROTO_MAGIC_RESPONSE_SUCCESS	0x02
#define PROTO_MAGIC_RESPONSE_FAILURE	0x03
#define PROTO_MAGIC_HEARTBEAT			0x04
// (Bit: 4-5) //
#define PROTO_MAGIC_FROM_COORDINATOR	0x10
#define PROTO_MAGIC_FROM_MASTER			0x20
#define PROTO_MAGIC_FROM_SLAVE			0x30
 // (Bit: 6-7) //
#define PROTO_MAGIC_TO_COORDINATOR		0x40
#define PROTO_MAGIC_TO_MASTER			0x80
#define PROTO_MAGIC_TO_SLAVE			0xC0

/*******************
 * Opcode (1 byte) *
 *******************/
// Coordinator-specific opcodes //
#define PROTO_OPCODE_REGISTER			0x00
#define PROTO_OPCODE_GET_CONFIG			0x01

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
	struct {
		size_t size;
		char *data;
	} buffer;

	size_t generateHeader( uint8_t magic, uint8_t to, uint8_t opcode, uint32_t length );

public:
	Protocol( Role role );
	bool init( size_t size, char *data );
	bool parseHeader( char *buf, size_t size, uint8_t &magic, uint8_t &from, uint8_t &opcode, uint32_t &length );

};

#endif

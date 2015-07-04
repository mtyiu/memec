#ifndef __PROTOCOL_HH__
#define __PROTOCOL_HH__

/************************
 *  Magic byte (1 byte) *
 ************************/
// (Bit: 0-3) //
#define PROTO_MAGIC_REQUEST				0x01
#define PROTO_MAGIC_RESPONSE_SUCCESS	0x02
#define PROTO_MAGIC_RESPONSE_FAILURE	0x03
#define PROTO_MAGIC_HEARTBEAT			0x04
// (Bit: 4-7) //
#define PROTO_MAGIC_FROM_COORDINATOR	0x10
#define PROTO_MAGIC_FROM_MASTER			0x20
#define PROTO_MAGIC_FROM_SLAVE			0x30
#define PROTO_MAGIC_TO_COORDINATOR		0x40
#define PROTO_MAGIC_TO_MASTER			0x50
#define PROTO_MAGIC_TO_SLAVE			0x60

/*******************
 * Opcode (1 byte) *
 *******************/
// Coordinator-specific opcodes //
#define PROTO_OPCODE_REGISTER			0x00
#define PROTO_OPCODE_GET_CONFIG			0x01

#endif

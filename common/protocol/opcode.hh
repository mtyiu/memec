#ifndef __COMMON_PROTOCOL_OPCODE_HH__
#define __COMMON_PROTOCOL_OPCODE_HH__

/*******************
 * Opcode (1 byte) *
 *******************/
#define PROTO_OPCODE_REGISTER                     0x00
#define PROTO_OPCODE_REGISTER_NAMED_PIPE          0x70

// Coordinator-specific opcodes (30-49) //
#define PROTO_OPCODE_SYNC                         0x31
#define PROTO_OPCODE_SERVER_CONNECTED             0x32
#define PROTO_OPCODE_SEAL_CHUNKS                  0x33
#define PROTO_OPCODE_FLUSH_CHUNKS                 0x34
#define PROTO_OPCODE_RECONSTRUCTION               0x35
#define PROTO_OPCODE_RECONSTRUCTION_UNSEALED      0x36
#define PROTO_OPCODE_SYNC_META                    0x37
#define PROTO_OPCODE_RELEASE_DEGRADED_LOCKS       0x38
#define PROTO_OPCODE_SERVER_RECONSTRUCTED         0x39
#define PROTO_OPCODE_BACKUP_SERVER_PROMOTED       0x40
#define PROTO_OPCODE_PARITY_MIGRATE               0x41

// Application <-> Client or Client <-> Server (0-19) //
#define PROTO_OPCODE_GET                          0x01
#define PROTO_OPCODE_SET                          0x02
#define PROTO_OPCODE_UPDATE                       0x03
#define PROTO_OPCODE_DELETE                       0x04
#define PROTO_OPCODE_UPDATE_CHECK                 0x05
#define PROTO_OPCODE_DELETE_CHECK                 0x06
#define PROTO_OPCODE_DEGRADED_GET                 0x07
#define PROTO_OPCODE_DEGRADED_UPDATE              0x08
#define PROTO_OPCODE_DEGRADED_DELETE              0x09
// Client <-> Server //
#define PROTO_OPCODE_DEGRADED_SET                 0x10
#define PROTO_OPCODE_DEGRADED_LOCK                0x11
#define PROTO_OPCODE_ACK_METADATA                 0x12
#define PROTO_OPCODE_ACK_PARITY_DELTA             0x13
#define PROTO_OPCODE_REVERT_DELTA                 0x14

// Client <-> Coordinator (20-29) //
#define PROTO_OPCODE_REMAPPING_LOCK               0x20

// Server <-> Server (50-69) //
#define PROTO_OPCODE_SEAL_CHUNK                   0x51
#define PROTO_OPCODE_UPDATE_CHUNK                 0x52
#define PROTO_OPCODE_DELETE_CHUNK                 0x53
#define PROTO_OPCODE_GET_CHUNK                    0x54
#define PROTO_OPCODE_SET_CHUNK                    0x55
#define PROTO_OPCODE_SET_CHUNK_UNSEALED           0x56
#define PROTO_OPCODE_FORWARD_CHUNK                0x57
#define PROTO_OPCODE_FORWARD_KEY                  0x58
#define PROTO_OPCODE_REMAPPED_UPDATE              0x59
#define PROTO_OPCODE_REMAPPED_DELETE              0x60
#define PROTO_OPCODE_BATCH_CHUNKS                 0x61
#define PROTO_OPCODE_BATCH_KEY_VALUES             0x62
#define PROTO_OPCODE_UPDATE_CHUNK_CHECK           0x63
#define PROTO_OPCODE_DELETE_CHUNK_CHECK           0x64

#define PROTO_UNINITIALIZED_INSTANCE              0

#endif

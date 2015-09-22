#ifndef __COMMON_PROTOCOL_PROTOCOL_HH__
#define __COMMON_PROTOCOL_PROTOCOL_HH__

/**************************************
 * Packet format:
 * - Magic byte (1 byte)
 * - Opcode (1 byte)
 * - Message length (4 bytes)
 * - Message ID (4 bytes)
 ***************************************/

/************************
 *  Magic byte (1 byte) *
 ************************/
// (Bit: 0-2) //
#define PROTO_MAGIC_HEARTBEAT                     0x00 // -----000
#define PROTO_MAGIC_REQUEST                       0x01 // -----001
#define PROTO_MAGIC_RESPONSE_SUCCESS              0x02 // -----010
#define PROTO_MAGIC_RESPONSE_FAILURE              0x03 // -----011
#define PROTO_MAGIC_ANNOUNCEMENT                  0x04 // -----100
#define PROTO_MAGIC_LOADING_STATS                 0x05 // -----101
#define PROTO_MAGIC_RESERVED_3                    0x06 // -----110
#define PROTO_MAGIC_RESERVED_4                    0x07 // -----111
// (Bit: 3-4) //
#define PROTO_MAGIC_FROM_APPLICATION              0x00 // ---00---
#define PROTO_MAGIC_FROM_COORDINATOR              0x08 // ---01---
#define PROTO_MAGIC_FROM_MASTER                   0x10 // ---10---
#define PROTO_MAGIC_FROM_SLAVE                    0x18 // ---11---
 // (Bit: 5-6) //
#define PROTO_MAGIC_TO_APPLICATION                0x00 // -00-----
#define PROTO_MAGIC_TO_COORDINATOR                0x20 // -01-----
#define PROTO_MAGIC_TO_MASTER                     0x40 // -10-----
#define PROTO_MAGIC_TO_SLAVE                      0x60 // -11-----
// (Bit: 7): Reserved //

/*******************
 * Opcode (1 byte) *
 *******************/
// Coordinator-specific opcodes //
#define PROTO_OPCODE_REGISTER                     0x30
#define PROTO_OPCODE_SYNC                         0x31
#define PROTO_OPCODE_SLAVE_CONNECTED              0x32
#define PROTO_OPCODE_MASTER_PUSH_STATS            0x33
#define PROTO_OPCODE_COORDINATOR_PUSH_STATS       0x34

// Application <-> Master or Master <-> Slave //
#define PROTO_OPCODE_GET                          0x01
#define PROTO_OPCODE_SET                          0x02
#define PROTO_OPCODE_UPDATE                       0x03
#define PROTO_OPCODE_DELETE                       0x04

// Master <-> Slave //
#define PROTO_OPCODE_REMAPPING_LOCK               0x10
#define PROTO_OPCODE_REMAPPING_SET                0x11

// Slave <-> Slave //
#define PROTO_OPCODE_UPDATE_CHUNK                 0x20
#define PROTO_OPCODE_DELETE_CHUNK                 0x21
#define PROTO_OPCODE_GET_CHUNK                    0x22
#define PROTO_OPCODE_SET_CHUNK                    0x23

/*********************
 * Key size (1 byte) *
 *********************/
#define MAXIMUM_KEY_SIZE 255
/***********************
 * Value size (3 byte) *
 ***********************/
 #define MAXIMUM_VALUE_SIZE 16777215

#include <map>
#include <stdint.h>
#include <arpa/inet.h>
#include "../../common/ds/key.hh"
#include "../../common/ds/metadata.hh"

enum Role {
	ROLE_APPLICATION,
	ROLE_COORDINATOR,
	ROLE_MASTER,
	ROLE_SLAVE
};

///////////////////////////////////////////////////////////////////////////////

#define PROTO_HEADER_SIZE 10
struct ProtocolHeader {
	uint8_t magic, from, to, opcode;
	uint32_t length; // Content length
	uint32_t id;
};

#define PROTO_HEARTBEAT_SIZE 16
struct HeartbeatHeader {
	uint32_t get;
	uint32_t set;
	uint32_t update;
	uint32_t del;
};

#define PROTO_SLAVE_SYNC_PER_SIZE 14
struct SlaveSyncHeader {
	uint8_t keySize;
	uint8_t opcode;
	uint32_t listId;
	uint32_t stripeId;
	uint32_t chunkId;
	char *key;
};

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

// vvvvvvvvvv For remapping vvvvvvvvvv //
#define PROTO_REMAPPING_LOCK_SIZE 9
struct RemappingLockHeader {
	uint32_t listId;
	uint32_t chunkId;
	uint8_t keySize;
	char *key;
};

#define PROTO_REMAPPING_SET_SIZE 13
struct RemappingSetHeader {
	uint32_t listId;
	uint32_t chunkId;
	bool needsForwarding;
	uint8_t keySize;
	uint32_t valueSize; // 3 bytes
	char *key;
	char *value;
};
// ^^^^^^^^^^ For remapping ^^^^^^^^^^ //

#define PROTO_CHUNK_UPDATE_SIZE 24
struct ChunkUpdateHeader {
	uint32_t listId;
	uint32_t stripeId;
	uint32_t chunkId;		 // ID of the chunk updated
	uint32_t offset;
	uint32_t length;
	uint32_t updatingChunkId; // ID of the chunk that is going to be updated
	char *delta;
};

#define PROTO_CHUNK_SIZE 12
struct ChunkHeader {
	uint32_t listId;
	uint32_t stripeId;
	uint32_t chunkId;
};

#define PROTO_CHUNK_DATA_SIZE 16
struct ChunkDataHeader {
	uint32_t listId;
	uint32_t stripeId;
	uint32_t chunkId;
	uint32_t size;
	char *data;
};

#define PROTO_ADDRESS_SIZE 6
struct AddressHeader {
	uint32_t addr;
	uint16_t port;
};

#define PROTO_LOAD_STATS_SIZE 8
struct LoadStatsHeader {
	uint32_t slaveGetCount;
	uint32_t slaveSetCount;
};

#define PROTO_BUF_MIN_SIZE		65536

///////////////////////////////////////////////////////////////////////////////

class Protocol {
protected:
	uint8_t from, to;

	size_t generateHeader( uint8_t magic, uint8_t to, uint8_t opcode, uint32_t length, uint32_t id, char *sendBuf = 0 );
	size_t generateKeyHeader( uint8_t magic, uint8_t to, uint8_t opcode, uint32_t id, uint8_t keySize, char *key );
	size_t generateKeyValueHeader( uint8_t magic, uint8_t to, uint8_t opcode, uint32_t id, uint8_t keySize, char *key, uint32_t valueSize, char *value, char *sendBuf = 0 );
	size_t generateKeyValueUpdateHeader( uint8_t magic, uint8_t to, uint8_t opcode, uint32_t id, uint8_t keySize, char *key, uint32_t valueUpdateOffset, uint32_t valueUpdateSize, char *valueUpdate = 0 );
	size_t generateRemappingLockHeader( uint8_t magic, uint8_t to, uint8_t opcode, uint32_t id, uint32_t listId, uint32_t chunkId, uint8_t keySize, char *key );
	size_t generateRemappingSetHeader( uint8_t magic, uint8_t to, uint8_t opcode, uint32_t id, uint32_t listId, uint32_t chunkId, bool needsForwarding, uint8_t keySize, char *key, uint32_t valueSize, char *value, char *sendBuf = 0 );
	size_t generateChunkUpdateHeader( uint8_t magic, uint8_t to, uint8_t opcode, uint32_t id, uint32_t listId, uint32_t stripeId, uint32_t chunkId, uint32_t offset, uint32_t length, uint32_t updatingChunkId, char *delta = 0, char *sendBuf = 0 );
	size_t generateChunkHeader( uint8_t magic, uint8_t to, uint8_t opcode, uint32_t id, uint32_t listId, uint32_t stripeId, uint32_t chunkId );
	size_t generateChunkDataHeader( uint8_t magic, uint8_t to, uint8_t opcode, uint32_t id, uint32_t listId, uint32_t stripeId, uint32_t chunkId, uint32_t chunkSize, char *chunkData );
	size_t generateHeartbeatMessage( uint8_t magic, uint8_t to, uint8_t opcode, uint32_t id, struct HeartbeatHeader &header, std::map<Key, OpMetadata> &ops, size_t &count );
	size_t generateAddressHeader( uint8_t magic, uint8_t to, uint8_t opcode, uint32_t id, uint32_t addr, uint16_t port );
	size_t generateLoadStatsHeader( uint8_t magic, uint8_t to, uint32_t id, uint32_t slaveGetCount, uint32_t slaveSetCount, uint32_t recordSize );

	bool parseHeader( uint8_t &magic, uint8_t &from, uint8_t &to, uint8_t &opcode, uint32_t &length, uint32_t &id, char *buf, size_t size );

	bool parseKeyHeader( size_t offset, uint8_t &keySize, char *&key, char *buf, size_t size );
	bool parseKeyValueHeader( size_t offset, uint8_t &keySize, char *&key, uint32_t &valueSize, char *&value, char *buf, size_t size );
	bool parseKeyValueUpdateHeader( size_t offset, uint8_t &keySize, char *&key, uint32_t &valueUpdateOffset, uint32_t &valueUpdateSize, char *buf, size_t size );
	bool parseKeyValueUpdateHeader( size_t offset, uint8_t &keySize, char *&key, uint32_t &valueUpdateOffset, uint32_t &valueUpdateSize, char *&valueUpdate, char *buf, size_t size );
	bool parseRemappingLockHeader( size_t offset, uint32_t &listId, uint32_t &chunkId, uint8_t &keySize, char *&key, char *buf, size_t size );
	bool parseRemappingSetHeader( size_t offset, uint32_t &listId, uint32_t &chunkId, bool &needsForwarding, uint8_t &keySize, char *&key, uint32_t &valueSize, char *&value, char *buf, size_t size );
	bool parseChunkUpdateHeader( size_t offset, uint32_t &listId, uint32_t &stripeId, uint32_t &chunkId, uint32_t &updateOffset, uint32_t &updateLength, uint32_t &updatingChunkId, char *buf, size_t size );
	bool parseChunkUpdateHeader( size_t offset, uint32_t &listId, uint32_t &stripeId, uint32_t &chunkId, uint32_t &updateOffset, uint32_t &updateLength, uint32_t &updatingChunkId, char *&delta, char *buf, size_t size );
	bool parseChunkHeader( size_t offset, uint32_t &listId, uint32_t &stripeId, uint32_t &chunkId, char *buf, size_t size );
	bool parseChunkDataHeader( size_t offset, uint32_t &listId, uint32_t &stripeId, uint32_t &chunkId, uint32_t &chunkSize, char *&chunkData, char *buf, size_t size );
	bool parseHeartbeatHeader( size_t offset, uint32_t &get, uint32_t &set, uint32_t &update, uint32_t &del, char *buf, size_t size );
	bool parseSlaveSyncHeader( size_t offset, uint8_t &keySize, uint8_t &opcode, uint32_t &listId, uint32_t &stripeId, uint32_t &chunkId, char *&key, char *buf, size_t size );
	bool parseAddressHeader( size_t offset, uint32_t &addr, uint16_t &port, char *buf, size_t size );
	bool parseLoadStatsHeader( size_t offset, uint32_t &slaveGetCount, uint32_t &slaveSetCount, char *buf, size_t size );

public:
	struct {
		size_t size;
		char *send;
		char *recv;
	} buffer;

	Protocol( Role role );
	bool init( size_t size = 0 );
	void free();
	bool parseHeader( struct ProtocolHeader &header, char *buf = 0, size_t size = 0 );
	bool parseKeyHeader( struct KeyHeader &header, char *buf = 0, size_t size = 0, size_t offset = 0 );
	bool parseKeyValueHeader( struct KeyValueHeader &header, char *buf = 0, size_t size = 0, size_t offset = 0 );
	bool parseKeyValueUpdateHeader( struct KeyValueUpdateHeader &header, bool withValueUpdate, char *buf = 0, size_t size = 0, size_t offset = 0 );
	bool parseRemappingLockHeader( struct RemappingLockHeader &header, char *buf = 0, size_t size = 0, size_t offset = 0 );
	bool parseRemappingSetHeader( struct RemappingSetHeader &header, char *buf = 0, size_t size = 0, size_t offset = 0 );
	bool parseChunkUpdateHeader( struct ChunkUpdateHeader &header, bool withDelta, char *buf = 0, size_t size = 0, size_t offset = 0 );
	bool parseChunkHeader( struct ChunkHeader &header, char *buf = 0, size_t size = 0, size_t offset = 0 );
	bool parseChunkDataHeader( struct ChunkDataHeader &header, char *buf = 0, size_t size = 0, size_t offset = 0 );
	bool parseHeartbeatHeader( struct HeartbeatHeader &header, char *buf = 0, size_t size = 0, size_t offset = 0 );
	bool parseSlaveSyncHeader( struct SlaveSyncHeader &header, size_t &bytes, char *buf = 0, size_t size = 0, size_t offset = PROTO_HEADER_SIZE + PROTO_HEARTBEAT_SIZE );
	bool parseAddressHeader( struct AddressHeader &header, char *buf = 0, size_t size = 0, size_t offset = 0 );
	bool parseLoadStatsHeader( struct LoadStatsHeader &header, char *buf = 0, size_t size = 0, size_t offset = 0 );

	static size_t getSuggestedBufferSize( uint32_t keySize, uint32_t chunkSize );
};

#endif

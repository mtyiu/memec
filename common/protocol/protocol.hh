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
#define PROTO_MAGIC_REMAPPING                     0x06 // -----110
#define PROTO_MAGIC_ACKNOWLEDGEMENT               0x07 // -----111
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
// Coordinator-specific opcodes (30-49) //
#define PROTO_OPCODE_REGISTER                     0x00
#define PROTO_OPCODE_SYNC                         0x31
#define PROTO_OPCODE_SLAVE_CONNECTED              0x32
#define PROTO_OPCODE_MASTER_PUSH_STATS            0x33
#define PROTO_OPCODE_COORDINATOR_PUSH_STATS       0x34
#define PROTO_OPCODE_SEAL_CHUNKS                  0x35
#define PROTO_OPCODE_FLUSH_CHUNKS                 0x36
#define PROTO_OPCODE_RECONSTRUCTION               0x37
#define PROTO_OPCODE_SYNC_META                    0x38
#define PROTO_OPCODE_RELEASE_DEGRADED_LOCKS       0x39
#define PROTO_OPCODE_SLAVE_RECONSTRUCTED          0x40
#define PROTO_OPCODE_BACKUP_SLAVE_PROMOTED        0x41
#define PROTO_OPCODE_PARITY_MIGRATE               0x42

// Application <-> Master or Master <-> Slave (0-19) //
#define PROTO_OPCODE_GET                          0x01
#define PROTO_OPCODE_SET                          0x02
#define PROTO_OPCODE_UPDATE                       0x03
#define PROTO_OPCODE_DELETE                       0x04
#define PROTO_OPCODE_REDIRECT_GET                 0x05
#define PROTO_OPCODE_REDIRECT_UPDATE              0x06
#define PROTO_OPCODE_REDIRECT_DELETE              0x07
#define PROTO_OPCODE_DEGRADED_GET                 0x08
#define PROTO_OPCODE_DEGRADED_UPDATE              0x09
#define PROTO_OPCODE_DEGRADED_DELETE              0x10
// Master <-> Slave //
#define PROTO_OPCODE_REMAPPING_SET                0x12
#define PROTO_OPCODE_DEGRADED_LOCK                0x13
#define PROTO_OPCODE_DEGRADED_UNLOCK              0x14
#define PROTO_OPCODE_ACK_METADATA                 0x15
#define PROTO_OPCODE_ACK_REQUEST                  0x16
#define PROTO_OPCODE_ACK_PARITY_DELTA             0x17

// Master <-> Coordinator (20-29) //
#define PROTO_OPCODE_REMAPPING_LOCK               0x20

// Slave <-> Slave (50-69) //
#define PROTO_OPCODE_REMAPPING_UNLOCK             0x50
#define PROTO_OPCODE_SEAL_CHUNK                   0x51
#define PROTO_OPCODE_UPDATE_CHUNK                 0x52
#define PROTO_OPCODE_DELETE_CHUNK                 0x53
#define PROTO_OPCODE_GET_CHUNK                    0x54
#define PROTO_OPCODE_GET_CHUNKS                   0x55
#define PROTO_OPCODE_SET_CHUNK                    0x56
#define PROTO_OPCODE_SET_CHUNK_UNSEALED           0x57

#define PROTO_UNINITIALIZED_INSTANCE              0

/*********************
 * Key size (1 byte) *
 *********************/
#define MAXIMUM_KEY_SIZE 255
/***********************
 * Value size (3 byte) *
 ***********************/
 #define MAXIMUM_VALUE_SIZE 16777215

#include <climits>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <stdint.h>
#include <arpa/inet.h>
#include "../ds/key.hh"
#include "../ds/key_value.hh"
#include "../ds/metadata.hh"
#include "../lock/lock.hh"
#include "../timestamp/timestamp.hh"

enum Role {
	ROLE_APPLICATION,
	ROLE_COORDINATOR,
	ROLE_MASTER,
	ROLE_SLAVE
};

///////////////////////////////////////////////////////////////////////////////

#define PROTO_HEADER_SIZE 16
struct ProtocolHeader {
	uint8_t magic, from, to, opcode;
	uint32_t length; // Content length
	uint16_t instanceId;
	uint32_t requestId;
	uint32_t timestamp;
};

//////////////
// Register //
//////////////
#define PROTO_ADDRESS_SIZE 6
struct AddressHeader {
	uint32_t addr;
	uint16_t port;
};

//////////////
// Recovery //
//////////////
#define PROTO_PROMOTE_BACKUP_SLAVE_SIZE 10
struct PromoteBackupSlaveHeader {
	uint32_t addr;
	uint16_t port;
	uint32_t count;
	uint32_t *metadata;
};

//////////////////////////////////////////
// Heartbeat & metadata synchronization //
//////////////////////////////////////////
#define PROTO_HEARTBEAT_SIZE 13
struct HeartbeatHeader {
    uint32_t timestamp;
	uint32_t sealed;
	uint32_t keys;
	bool isLast;
};

#define PROTO_METADATA_SIZE 12
struct MetadataHeader {
	uint32_t listId;
	uint32_t stripeId;
	uint32_t chunkId;
};

#define PROTO_KEY_OP_METADATA_SIZE 18
struct KeyOpMetadataHeader {
	uint8_t keySize;
	uint8_t opcode;
	uint32_t listId;
	uint32_t stripeId;
	uint32_t chunkId;
	uint32_t timestamp;
	char *key;
};

#define PROTO_SLAVE_REMAPPED_PARITY_SIZE PROTO_ADDRESS_SIZE

//////////////////////////
// Load synchronization //
//////////////////////////
#define PROTO_LOAD_STATS_SIZE 12
struct LoadStatsHeader {
	uint32_t slaveGetCount;
	uint32_t slaveSetCount;
	uint32_t slaveOverloadCount;
};

///////////////////////
// Normal operations //
///////////////////////
#define PROTO_KEY_SIZE 1
struct KeyHeader {
	uint8_t keySize;
	char *key;
};

#define PROTO_KEY_SLAVE_SIZE 17
struct KeySlaveHeader {
    uint8_t keySize;
    char *key;
};

#define PROTO_KEY_BACKUP_BASE_SIZE       2
#define PROTO_KEY_BACKUP_FOR_DATA_SIZE   17
#define PROTO_KEY_BACKUP_SEALED_SIZE     12
struct KeyBackupHeader {
    bool isParity;
	uint8_t keySize;
    uint32_t timestamp;      // Only for data servers
    uint32_t listId;         // Only for data servers
    uint32_t stripeId;       // Only for data servers
    uint32_t chunkId;        // Only for data servers
	bool isSealed;           // Only for data servers
	uint32_t sealedListId;   // Only for data servers && isSealed
	uint32_t sealedStripeId; // Only for data servers && isSealed
	uint32_t sealedChunkId;  // Only for data servers && isSealed
    char *key;
};

#define PROTO_CHUNK_KEY_SIZE 13
struct ChunkKeyHeader {
	uint32_t listId;
	uint32_t stripeId;
	uint32_t chunkId;
	uint8_t keySize;
	char *key;
};

#define PROTO_CHUNK_KEY_VALUE_UPDATE_SIZE 22
struct ChunkKeyValueUpdateHeader {
	uint32_t listId;
	uint32_t stripeId;
	uint32_t chunkId;
	uint8_t keySize;
	uint32_t valueUpdateSize;   // 3 bytes
	uint32_t valueUpdateOffset; // 3 bytes
	uint32_t chunkUpdateOffset; // 3 bytes
	char *key;
	char *valueUpdate;
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

///////////////
// Remapping //
///////////////
#define PROTO_REMAPPING_LOCK_SIZE 5
struct RemappingLockHeader {
	uint32_t remappedCount;
	uint8_t keySize;
	char *key;
	uint32_t *original;
	uint32_t *remapped;
};

#define PROTO_REMAPPING_SET_SIZE 16
struct RemappingSetHeader {
	uint32_t listId;
	uint32_t chunkId;
	uint32_t remappedCount;
	uint8_t keySize;
	uint32_t valueSize; // 3 bytes
	char *key;
	char *value;
	uint32_t *original;
	uint32_t *remapped;
};

////////////////////////
// Degraded operation //
////////////////////////
#define PROTO_DEGRADED_LOCK_REQ_SIZE 5
struct DegradedLockReqHeader {
	uint32_t reconstructedCount;
	uint8_t keySize;
	char *key;
	uint32_t *original;
	uint32_t *reconstructed;
};

// Size
#define PROTO_DEGRADED_LOCK_RES_BASE_SIZE   2 // type: 1, 2, 3, 4, 5
#define PROTO_DEGRADED_LOCK_RES_LOCK_SIZE   9 // type: 1, 2
#define PROTO_DEGRADED_LOCK_RES_REMAP_SIZE  4 // type: 4
#define PROTO_DEGRADED_LOCK_RES_NOT_SIZE    0 // type: 3, 5
// Type
#define PROTO_DEGRADED_LOCK_RES_IS_LOCKED   1
#define PROTO_DEGRADED_LOCK_RES_WAS_LOCKED  2
#define PROTO_DEGRADED_LOCK_RES_NOT_LOCKED  3
#define PROTO_DEGRADED_LOCK_RES_REMAPPED    4
#define PROTO_DEGRADED_LOCK_RES_NOT_EXIST   5
struct DegradedLockResHeader {
	uint8_t type;                  // type: 1, 2, 3, 4, 5
	uint8_t keySize;               // type: 1, 2, 3, 4, 5
	char *key;                     // type: 1, 2, 3, 4, 5

	bool isSealed;                 // type: 1, 2
	uint32_t stripeId;             // type: 1, 2
	uint32_t reconstructedCount;   // type: 1, 2
	uint32_t remappedCount;        // type: 4
	uint32_t *original;            // type: 1, 2, 4
	uint32_t *reconstructed;       // type: 1, 2
	uint32_t *remapped;            // type: 4
};

#define PROTO_DEGRADED_REQ_BASE_SIZE 9
struct DegradedReqHeader {
	bool isSealed;
	uint32_t stripeId;
	uint32_t reconstructedCount;
	uint32_t *original;
	uint32_t *reconstructed;
	union {
		struct KeyHeader key;
		struct KeyValueUpdateHeader keyValueUpdate;
	} data;
};

// For retrieving key-value pair in unsealed chunks from parity slave
#define PROTO_LIST_STRIPE_KEY_SIZE 9
struct ListStripeKeyHeader {
	uint32_t listId;
	uint32_t chunkId;
	uint8_t keySize;
	char *key;
};

// For asking slaves to send back the modified reconstructed chunks to the overloaded slave
#define PROTO_DEGRADED_RELEASE_REQ_SIZE 20
struct DegradedReleaseReqHeader {
	uint32_t srcListId;
	uint32_t srcStripeId;
	uint32_t srcChunkId;
	uint32_t dstListId;
	uint32_t dstChunkId;
};

#define PROTO_DEGRADED_RELEASE_RES_SIZE 4
struct DegradedReleaseResHeader {
	uint32_t count;
};

////////////////////
// Reconstruction //
////////////////////
#define PROTO_RECONSTRUCTION_SIZE 12
struct ReconstructionHeader {
	uint32_t listId;
	uint32_t chunkId;
	uint32_t numStripes;
	uint32_t *stripeIds;
};

//////////
// Seal //
//////////
#define PROTO_CHUNK_SEAL_SIZE 16
#define PROTO_CHUNK_SEAL_DATA_SIZE 5
struct ChunkSealHeader {
	uint32_t listId;
	uint32_t stripeId;
	uint32_t chunkId;
	uint32_t count;
};
struct ChunkSealHeaderData {
	uint8_t keySize;
	uint32_t offset;
	char *key;
};

//////////////////////
// Chunk operations //
//////////////////////
#define PROTO_CHUNK_SIZE 12
struct ChunkHeader {
	uint32_t listId;
	uint32_t stripeId;
	uint32_t chunkId;
};

#define PROTO_CHUNKS_SIZE 12
struct ChunksHeader {
	uint32_t listId;
	uint32_t chunkId;
	uint32_t numStripes;
	uint32_t *stripeIds;
};

#define PROTO_CHUNK_DATA_SIZE 20
struct ChunkDataHeader {
	uint32_t listId;
	uint32_t stripeId;
	uint32_t chunkId;
	uint32_t size;
	uint32_t offset;
	char *data;
};

#define PROTO_CHUNK_KEY_VALUE_SIZE 21
struct ChunkKeyValueHeader {
	uint32_t listId;
	uint32_t stripeId;
	uint32_t chunkId;
	uint32_t deleted;
	uint32_t count;
	bool isCompleted;
};

#define PROTO_BUF_MIN_SIZE		65536

/////////////////////
// Acknowledgement //
/////////////////////
#define PROTO_ACK_BASE_SIZE 8
struct AcknowledgementHeader {
    uint32_t fromTimestamp;
    uint32_t toTimestamp;
};

#define PROTO_ACK_PARITY_DELTA_SIZE 10
struct ParityDeltaAcknowledgementHeader {
	uint32_t fromTimestamp;
	uint32_t toTimestamp;
	uint16_t targetId;
};

///////////////////////////////////////////////////////////////////////////////

class Protocol {
protected:
	uint8_t from, to;
	// ---------- protocol.cc ----------
	size_t generateHeader(
		uint8_t magic, uint8_t to, uint8_t opcode,
		uint32_t length, uint16_t instanceId, uint32_t requestId,
		char *sendBuf = 0, uint32_t requestTimestamp = 0
	);
	bool parseHeader(
		uint8_t &magic, uint8_t &from, uint8_t &to, uint8_t &opcode,
		uint32_t &length, uint16_t &instanceId, uint32_t &requestId,
		uint32_t &requestTimestamp, char *buf, size_t size
	);
	Timestamp localTimestamp;
	//////////////
	// Register //
	//////////////
	// ---------- address_protocol.cc ----------
	size_t generateAddressHeader(
		uint8_t magic, uint8_t to, uint8_t opcode, uint16_t instanceId, uint32_t requestId,
		uint32_t addr, uint16_t port, char* buf = 0
	);
	bool parseAddressHeader(
		size_t offset, uint32_t &addr, uint16_t &port,
		char *buf, size_t size
	);

	////////////////////
	// Reconstruction //
	////////////////////
	// ---------- address_protocol.cc ----------
	size_t generateSrcDstAddressHeader(
		uint8_t magic, uint8_t to, uint8_t opcode, uint16_t instanceId, uint32_t requestId,
		uint32_t srcAddr, uint16_t srcPort,
		uint32_t dstAddr, uint16_t dstPort
	);
	// ---------- recovery_protocol.cc ----------
	size_t generatePromoteBackupSlaveHeader(
		uint8_t magic, uint8_t to, uint8_t opcode, uint16_t instanceId, uint32_t requestId,
		uint32_t addr, uint16_t port,
		std::unordered_set<Metadata> &chunks,
		std::unordered_set<Metadata>::iterator &it,
		bool &isCompleted
	);
	size_t generatePromoteBackupSlaveHeader(
		uint8_t magic, uint8_t to, uint8_t opcode, uint16_t instanceId, uint32_t requestId,
		uint32_t addr, uint16_t port, uint32_t numReconstructed
	);
	bool parsePromoteBackupSlaveHeader(
		size_t offset, uint32_t &addr, uint16_t &port, uint32_t &count,
		uint32_t *&metadata,
		char *buf, size_t size
	);
	bool parsePromoteBackupSlaveHeader(
		size_t offset, uint32_t &addr, uint16_t &port, uint32_t &numReconstructed,
		char *buf, size_t size
	);

	//////////////////////////////////////////
	// Heartbeat & metadata synchronization //
	//////////////////////////////////////////
	// ---------- heartbeat_protocol.cc ----------
	size_t generateHeartbeatMessage(
		uint8_t magic, uint8_t to, uint8_t opcode, uint16_t instanceId, uint32_t requestId, uint32_t timestamp,
		LOCK_T *sealedLock, std::unordered_set<Metadata> &sealed, uint32_t &sealedCount,
		LOCK_T *opsLock, std::unordered_map<Key, OpMetadata> &ops, uint32_t &opsCount,
		bool &isCompleted
	);
	size_t generateHeartbeatMessage(
		uint8_t magic, uint8_t to, uint8_t opcode, uint16_t instanceId, uint32_t requestId,
        uint32_t timestamp,
        uint32_t sealed,
        uint32_t keys,
        bool isLast
	);
	bool parseHeartbeatHeader(
		size_t offset, uint32_t &timestamp, uint32_t &sealed, uint32_t &keys, bool &isLast,
		char *buf, size_t size
	);
	bool parseMetadataHeader(
		size_t offset, uint32_t &listId, uint32_t &stripeId, uint32_t &chunkId,
		char *buf, size_t size
	);
	bool parseKeyOpMetadataHeader(
		size_t offset, uint8_t &keySize, uint8_t &opcode,
		uint32_t &listId, uint32_t &stripeId, uint32_t &chunkId, uint32_t &timestamp,
		char *&key, char *buf, size_t size
	);
	// ---------- fault_protocol.cc ----------
	size_t generateMetadataBackupMessage(
		uint8_t magic, uint8_t to, uint8_t opcode,
		uint16_t instanceId, uint32_t requestId,
		uint32_t addr, uint16_t port,
		LOCK_T *lock,
		std::unordered_multimap<uint32_t, Metadata> &sealed, uint32_t &sealedCount,
		std::unordered_map<Key, MetadataBackup> &ops, uint32_t &opsCount,
		bool &isCompleted
	);

	//////////////////////////
	// Load synchronization //
	//////////////////////////
	// ---------- load_protocol.cc ----------
	size_t generateLoadStatsHeader(
		uint8_t magic, uint8_t to, uint16_t instanceId, uint32_t requestId,
		uint32_t slaveGetCount, uint32_t slaveSetCount, uint32_t slaveOverloadCount,
		uint32_t recordSize, uint32_t slaveAddrSize
	);
	bool parseLoadStatsHeader(
		size_t offset, uint32_t &slaveGetCount, uint32_t &slaveSetCount, uint32_t &slaveOverloadCount,
		char *buf, size_t size
	);

	///////////////////////
	// Normal operations //
	///////////////////////
	// ---------- normal_protocol.cc ----------
	size_t generateKeyHeader(
		uint8_t magic, uint8_t to, uint8_t opcode, uint16_t instanceId, uint32_t requestId,
		uint8_t keySize, char *key, char *sendBuf = 0, uint32_t timestamp = 0
	);
	bool parseKeyHeader( size_t offset, uint8_t &keySize, char *&key, char *buf, size_t size );

	size_t generateKeyBackupHeader(
		uint8_t magic, uint8_t to, uint8_t opcode, uint16_t instanceId, uint32_t requestId,
		uint32_t timestamp,
		uint32_t listId, uint32_t stripeId, uint32_t chunkId,
		uint32_t sealedListId, uint32_t sealedStripeId, uint32_t sealedChunkId,
		uint8_t keySize, char *key, char *sendBuf = 0
	);
	size_t generateKeyBackupHeader(
		uint8_t magic, uint8_t to, uint8_t opcode, uint16_t instanceId, uint32_t requestId,
        uint32_t timestamp,
        uint32_t listId, uint32_t stripeId, uint32_t chunkId,
		uint8_t keySize, char *key, char *sendBuf = 0
	);
    size_t generateKeyBackupHeader(
		uint8_t magic, uint8_t to, uint8_t opcode, uint16_t instanceId, uint32_t requestId,
		uint8_t keySize, char *key, char *sendBuf = 0
	);
	bool parseKeyBackupHeader(
        size_t offset, bool &isParity, uint8_t &keySize,
        uint32_t &timestamp, uint32_t &listId, uint32_t &stripeId, uint32_t &chunkId,
		bool &isSealed, uint32_t &sealedListId, uint32_t &sealedStripeId, uint32_t &sealedChunkId,
        char *&key,
        char *buf, size_t size
    );

	size_t generateChunkKeyHeader(
		uint8_t magic, uint8_t to, uint8_t opcode, uint16_t instanceId, uint32_t requestId,
		uint32_t listId, uint32_t stripeId, uint32_t chunkId,
		uint8_t keySize, char *key, char *sendBuf = 0, uint32_t timestamp = 0
	);
	bool parseChunkKeyHeader(
		size_t offset, uint32_t &listId, uint32_t &stripeId, uint32_t &chunkId,
		uint8_t &keySize, char *&key,
		char *buf, size_t size
	);

	size_t generateChunkKeyValueUpdateHeader(
		uint8_t magic, uint8_t to, uint8_t opcode, uint16_t instanceId, uint32_t requestId,
		uint32_t listId, uint32_t stripeId, uint32_t chunkId,
		uint8_t keySize, char *key, uint32_t valueUpdateOffset, uint32_t valueUpdateSize,
		uint32_t chunkUpdateOffset, char *valueUpdate, char *sendBuf = 0,
		uint32_t timestamp = 0
	);
	bool parseChunkKeyValueUpdateHeader(
		size_t offset, uint32_t &listId, uint32_t &stripeId, uint32_t &chunkId,
		uint8_t &keySize, char *&key,
		uint32_t &valueUpdateOffset, uint32_t &valueUpdateSize, uint32_t &chunkUpdateOffset,
		char *buf, size_t size
	);
	bool parseChunkKeyValueUpdateHeader(
		size_t offset, uint32_t &listId, uint32_t &stripeId, uint32_t &chunkId,
		uint8_t &keySize, char *&key,
		uint32_t &valueUpdateOffset, uint32_t &valueUpdateSize, uint32_t &chunkUpdateOffset, char *&valueUpdate,
		char *buf, size_t size
	);

	size_t generateKeyValueHeader(
		uint8_t magic, uint8_t to, uint8_t opcode, uint16_t instanceId, uint32_t requestId,
		uint8_t keySize, char *key, uint32_t valueSize, char *value, char *sendBuf = 0,
		uint32_t timestamp = 0
	);
	bool parseKeyValueHeader(
		size_t offset, uint8_t &keySize, char *&key,
		uint32_t &valueSize, char *&value,
		char *buf, size_t size
	);

	size_t generateKeyValueUpdateHeader(
		uint8_t magic, uint8_t to, uint8_t opcode, uint16_t instanceId, uint32_t requestId,
		uint8_t keySize, char *key,
		uint32_t valueUpdateOffset, uint32_t valueUpdateSize, char *valueUpdate = 0,
		uint32_t timestamp = 0
	);
	bool parseKeyValueUpdateHeader(
		size_t offset, uint8_t &keySize, char *&key,
		uint32_t &valueUpdateOffset, uint32_t &valueUpdateSize,
		char *buf, size_t size
	);
	bool parseKeyValueUpdateHeader(
		size_t offset, uint8_t &keySize, char *&key,
		uint32_t &valueUpdateOffset, uint32_t &valueUpdateSize, char *&valueUpdate,
		char *buf, size_t size
	);

	size_t generateChunkUpdateHeader(
		uint8_t magic, uint8_t to, uint8_t opcode, uint16_t instanceId, uint32_t requestId,
		uint32_t listId, uint32_t stripeId, uint32_t chunkId,
		uint32_t offset, uint32_t length, uint32_t updatingChunkId,
		char *delta = 0, char *sendBuf = 0, uint32_t timestamp = 0
	);
	bool parseChunkUpdateHeader(
		size_t offset, uint32_t &listId, uint32_t &stripeId, uint32_t &chunkId,
		uint32_t &updateOffset, uint32_t &updateLength, uint32_t &updatingChunkId,
		char *buf, size_t size
	);
	bool parseChunkUpdateHeader(
		size_t offset, uint32_t &listId, uint32_t &stripeId, uint32_t &chunkId,
		uint32_t &updateOffset, uint32_t &updateLength, uint32_t &updatingChunkId,
		char *&delta, char *buf, size_t size
	);

	///////////////
	// Remapping //
	///////////////
	// ---------- remap_protocol.cc ----------
	size_t generateRemappingLockHeader(
		uint8_t magic, uint8_t to, uint8_t opcode, uint16_t instanceId, uint32_t requestId,
		uint32_t *original, uint32_t *remapped, uint32_t remappedCount,
		uint8_t keySize, char *key
	);
	bool parseRemappingLockHeader(
		size_t offset,
		uint32_t *&original, uint32_t *&remapped, uint32_t &remappedCount,
		uint8_t &keySize, char *&key,
		char *buf, size_t size
	);

	size_t generateRemappingSetHeader(
		uint8_t magic, uint8_t to, uint8_t opcode, uint16_t instanceId, uint32_t requestId,
		uint32_t listId, uint32_t chunkId,
		uint32_t *original, uint32_t *remapped, uint32_t remappedCount,
		uint8_t keySize, char *key,
		uint32_t valueSize, char *value,
		char *sendBuf = 0
	);
	bool parseRemappingSetHeader(
		size_t offset, uint32_t &listId, uint32_t &chunkId,
		uint32_t *&original, uint32_t *&remapped, uint32_t &remappedCount,
		uint8_t &keySize, char *&key,
		uint32_t &valueSize, char *&value,
		char *buf, size_t size
	);

	////////////////////////
	// Degraded operation //
	////////////////////////
	// ---------- degraded_protocol.cc ----------
	size_t generateDegradedLockReqHeader(
		uint8_t magic, uint8_t to, uint8_t opcode, uint16_t instanceId, uint32_t requestId,
		uint32_t *original, uint32_t *reconstructed, uint32_t reconstructedCount,
		uint8_t keySize, char *key
	);
	bool parseDegradedLockReqHeader(
		size_t offset,
		uint32_t *&original, uint32_t *&reconstructed, uint32_t &reconstructedCount,
		uint8_t &keySize, char *&key, char *buf, size_t size
	);

	size_t generateDegradedLockResHeader(
		uint8_t magic, uint8_t to, uint8_t opcode, uint16_t instanceId, uint32_t requestId,
		uint32_t length, uint8_t type, uint8_t keySize, char *key, char *&buf
	);
	size_t generateDegradedLockResHeader(
		uint8_t magic, uint8_t to, uint8_t opcode, uint16_t instanceId, uint32_t requestId,
		bool isLocked, uint8_t keySize, char *key,
		bool isSealed, uint32_t stripeId,
		uint32_t *original, uint32_t *reconstructed, uint32_t reconstructedCount
	);
	size_t generateDegradedLockResHeader(
		uint8_t magic, uint8_t to, uint8_t opcode, uint16_t instanceId, uint32_t requestId,
		bool exist, uint8_t keySize, char *key
	);
	size_t generateDegradedLockResHeader(
		uint8_t magic, uint8_t to, uint8_t opcode, uint16_t instanceId, uint32_t requestId,
		uint8_t keySize, char *key,
		uint32_t *original, uint32_t *remapped, uint32_t remappedCount
	);
	bool parseDegradedLockResHeader(
		size_t offset, uint8_t &type,
		uint8_t &keySize, char *&key,
		char *buf, size_t size
	);
	bool parseDegradedLockResHeader(
		size_t offset, bool &isSealed,
		uint32_t &stripeId,
		uint32_t *&original, uint32_t *&reconstructed, uint32_t &reconstructedCount,
		char *buf, size_t size
	);
	bool parseDegradedLockResHeader(
		size_t offset,
		uint32_t *&original, uint32_t *&remapped, uint32_t &remappedCount,
		char *buf, size_t size
	);

	size_t generateDegradedReqHeader(
		uint8_t magic, uint8_t to, uint8_t opcode, uint16_t instanceId, uint32_t requestId,
		bool isSealed, uint32_t stripeId,
		uint32_t *original, uint32_t *reconstructed, uint32_t reconstructedCount,
		uint8_t keySize, char *key
	);
	size_t generateDegradedReqHeader(
		uint8_t magic, uint8_t to, uint8_t opcode, uint16_t instanceId, uint32_t requestId,
		bool isSealed, uint32_t stripeId,
		uint32_t *original, uint32_t *reconstructed, uint32_t reconstructedCount,
		uint8_t keySize, char *key,
		uint32_t valueUpdateOffset, uint32_t valueUpdateSize, char *valueUpdate
	);
	bool parseDegradedReqHeader(
		size_t offset,
		bool &isSealed, uint32_t &stripeId,
		uint32_t *&original, uint32_t *&reconstructed, uint32_t &reconstructedCount,
		char *buf, size_t size
	);

	size_t generateListStripeKeyHeader(
		uint8_t magic, uint8_t to, uint8_t opcode, uint16_t instanceId, uint32_t requestId,
		uint32_t listId, uint32_t chunkId, uint8_t keySize, char *key
	);
	bool parseListStripeKeyHeader(
		size_t offset,
		uint32_t &listId, uint32_t &chunkId, uint8_t &keySize, char *&key,
		char *buf, size_t size
	);

	size_t generateDegradedReleaseReqHeader(
		uint8_t magic, uint8_t to, uint8_t opcode, uint16_t instanceId, uint32_t requestId,
		std::vector<Metadata> &chunks,
		bool &isCompleted
	);
    #define parseDegradedReleaseReqHeader parseChunkHeader

	size_t generateDegradedReleaseResHeader(
		uint8_t magic, uint8_t to, uint8_t opcode, uint16_t instanceId, uint32_t requestId,
		uint32_t count
	);
	bool parseDegradedReleaseResHeader(
		size_t offset,
		uint32_t &count,
		char *buf, size_t size
	);

	////////////////////
	// Reconstruction //
	////////////////////
	// ---------- recovery_protocol.cc ----------
	size_t generateReconstructionHeader(
		uint8_t magic, uint8_t to, uint8_t opcode, uint16_t instanceId, uint32_t requestId,
		uint32_t listId, uint32_t chunkId,
		std::unordered_set<uint32_t> &stripeIds,
		std::unordered_set<uint32_t>::iterator &it,
		uint32_t numChunks,
		bool &isCompleted
	);
	size_t generateReconstructionHeader(
		uint8_t magic, uint8_t to, uint8_t opcode, uint16_t instanceId, uint32_t requestId,
		uint32_t listId, uint32_t chunkId,
		uint32_t numChunks
	);
	bool parseReconstructionHeader(
		size_t offset,
		uint32_t &listId, uint32_t &chunkId, uint32_t &numStripes,
		uint32_t *&stripeIds,
		char *buf, size_t size
	);
	bool parseReconstructionHeader(
		size_t offset,
		uint32_t &listId, uint32_t &chunkId, uint32_t &numStripes,
		char *buf, size_t size
	);

	//////////
	// Seal //
	//////////
	// ---------- seal_protocol.cc ----------
	size_t generateChunkSealHeader(
		uint8_t magic, uint8_t to, uint8_t opcode, uint16_t instanceId, uint32_t requestId,
		uint32_t listId, uint32_t stripeId, uint32_t chunkId,
		uint32_t count, uint32_t dataLength, char *sendBuf = 0
	);
	bool parseChunkSealHeader(
		size_t offset, uint32_t &listId, uint32_t &stripeId, uint32_t &chunkId,
		uint32_t &count, char *buf, size_t size
	);
	bool parseChunkSealHeaderData(
		size_t offset, uint8_t &keySize, uint32_t &keyOffset, char *&key,
		char *buf, size_t size
	);

	//////////////////////
	// Chunk operations //
	//////////////////////
	// ---------- chunk_protocol.cc ----------
	size_t generateChunkHeader(
		uint8_t magic, uint8_t to, uint8_t opcode, uint16_t instanceId, uint32_t requestId,
		uint32_t listId, uint32_t stripeId, uint32_t chunkId, char *sendBuf = 0
	);
	bool parseChunkHeader(
		size_t offset, uint32_t &listId, uint32_t &stripeId, uint32_t &chunkId,
		char *buf, size_t size
	);

	size_t generateChunksHeader(
		uint8_t magic, uint8_t to, uint8_t opcode, uint16_t instanceId, uint32_t requestId,
		uint32_t listId, uint32_t chunkId, std::vector<uint32_t> &stripeIds,
		uint32_t &count
	);
	bool parseChunksHeader(
		size_t offset, uint32_t &listId, uint32_t &chunkId,
		uint32_t &numStripes, uint32_t *&stripeIds,
		char *buf, size_t size
	);

	size_t generateChunkDataHeader(
		uint8_t magic, uint8_t to, uint8_t opcode, uint16_t instanceId, uint32_t requestId,
		uint32_t listId, uint32_t stripeId, uint32_t chunkId,
		uint32_t chunkSize, uint32_t chunkOffset, char *chunkData
	);
	bool parseChunkDataHeader(
		size_t offset, uint32_t &listId, uint32_t &stripeId, uint32_t &chunkId,
		uint32_t &chunkSize, uint32_t &chunkOffset, char *&chunkData,
		char *buf, size_t size
	);

	size_t generateChunkKeyValueHeader(
		uint8_t magic, uint8_t to, uint8_t opcode, uint16_t instanceId, uint32_t requestId,
		uint32_t listId, uint32_t stripeId, uint32_t chunkId,
		std::unordered_map<Key, KeyValue> *values,
		std::unordered_multimap<Metadata, Key> *metadataRev,
		std::unordered_set<Key> *deleted,
		LOCK_T *lock,
		bool &isCompleted
	);
	bool parseChunkKeyValueHeader(
		size_t offset, uint32_t &listId, uint32_t &stripeId, uint32_t &chunkId,
		uint32_t &deleted, uint32_t &count, bool &isCompleted, char *&dataPtr,
		char *buf, size_t size
	);

	/////////////////////
	// Acknowledgement //
	/////////////////////
	// ---------- ack_protocol.cc ----------
	size_t generateAcknowledgementHeader(
		uint8_t magic, uint8_t to, uint8_t opcode, uint16_t instanceId, uint32_t requestId,
		uint32_t fromTimestamp, uint32_t toTimestamp, char* buf = 0
	);
	bool parseAcknowledgementHeader(
		size_t offset, uint32_t &fromTimestamp, uint32_t &toTimestamp,
		char *buf, size_t size
	);
	size_t generateParityDeltaAcknowledgementHeader(
		uint8_t magic, uint8_t to, uint8_t opcode, uint16_t instanceId, uint32_t requestId,
		uint32_t fromTimestamp, uint32_t toTimestamp, uint16_t targetId, char* buf = 0
	);
	bool parseParityDeltaAcknowledgementHeader(
		size_t offset, uint32_t &fromTimestamp, uint32_t &toTimestamp, uint16_t &targetId,
		char *buf, size_t size
	);

public:
	struct {
		size_t size;
		char *send;
		char *recv;
	} buffer;

	// ---------- protocol.cc ----------
	Protocol( Role role );
	bool init( size_t size = 0 );
	void free();
	bool parseHeader( struct ProtocolHeader &header, char *buf = 0, size_t size = 0 );
	static size_t getSuggestedBufferSize( uint32_t keySize, uint32_t chunkSize );

	//////////////
	// Register //
	//////////////
	// ---------- address_protocol.cc ----------
	bool parseAddressHeader(
		struct AddressHeader &header,
		char *buf = 0, size_t size = 0, size_t offset = 0
	);
	bool parseSrcDstAddressHeader(
		struct AddressHeader &srcHeader,
		struct AddressHeader &dstHeader,
		char *buf = 0, size_t size = 0, size_t offset = 0
	);
	////////////////////
	// Reconstruction //
	////////////////////
	// ---------- recovery_protocol.cc ----------
	bool parsePromoteBackupSlaveHeader(
		struct PromoteBackupSlaveHeader &header, bool isRequest,
		char *buf = 0, size_t size = 0, size_t offset = 0
	);
	//////////////////////////////////////////
	// Heartbeat & metadata synchronization //
	//////////////////////////////////////////
	// ---------- heartbeat_protocol.cc ----------
	bool parseHeartbeatHeader(
		struct HeartbeatHeader &header,
		char *buf = 0, size_t size = 0, size_t offset = 0
	);
	bool parseMetadataHeader(
		struct MetadataHeader &header, size_t &bytes,
		char *buf = 0, size_t size = 0, size_t offset = 0
	);
	bool parseKeyOpMetadataHeader(
		struct KeyOpMetadataHeader &header, size_t &bytes,
		char *buf = 0, size_t size = 0, size_t offset = 0
	);
	// ---------- fault_protocol.cc ----------
	bool parseMetadataBackupMessage(
		struct AddressHeader &address,
		struct HeartbeatHeader &heartbeat,
		char *buf = 0, size_t size = 0, size_t offset = 0
	);
	//////////////////////////
	// Load synchronization //
	//////////////////////////
	// ---------- load_protocol.cc ----------
	bool parseLoadStatsHeader(
		struct LoadStatsHeader &header,
		char *buf = 0, size_t size = 0, size_t offset = 0
	);
	///////////////////////
	// Normal operations //
	///////////////////////
	// ---------- normal_protocol.cc ----------
	bool parseKeyHeader(
		struct KeyHeader &header,
		char *buf = 0, size_t size = 0, size_t offset = 0
	);
	bool parseKeyBackupHeader(
		struct KeyBackupHeader &header,
		char *buf = 0, size_t size = 0, size_t offset = 0
	);
	bool parseChunkKeyHeader(
		struct ChunkKeyHeader &header,
		char *buf = 0, size_t size = 0, size_t offset = 0
	);
	bool parseChunkKeyValueUpdateHeader(
		struct ChunkKeyValueUpdateHeader &header, bool withValueUpdate,
		char *buf = 0, size_t size = 0, size_t offset = 0
	);
	bool parseKeyValueHeader(
		struct KeyValueHeader &header,
		char *buf = 0, size_t size = 0, size_t offset = 0
	);
	bool parseKeyValueUpdateHeader(
		struct KeyValueUpdateHeader &header, bool withValueUpdate,
		char *buf = 0, size_t size = 0, size_t offset = 0
	);
	bool parseChunkUpdateHeader(
		struct ChunkUpdateHeader &header, bool withDelta,
		char *buf = 0, size_t size = 0, size_t offset = 0
	);
	///////////////
	// Remapping //
	///////////////
	// ---------- remap_protocol.cc ----------
	bool parseRemappingLockHeader(
		struct RemappingLockHeader &header,
		char *buf = 0, size_t size = 0, size_t offset = 0
	);
	bool parseRemappingSetHeader(
		struct RemappingSetHeader &header,
		char *buf = 0, size_t size = 0, size_t offset = 0,
		struct sockaddr_in *target = 0
	);
	////////////////////////
	// Degraded operation //
	////////////////////////
	// ---------- degraded_protocol.cc ----------
	bool parseDegradedLockReqHeader(
		struct DegradedLockReqHeader &header,
		char *buf = 0, size_t size = 0, size_t offset = 0
	);
	bool parseDegradedLockResHeader(
		struct DegradedLockResHeader &header,
		char *buf = 0, size_t size = 0, size_t offset = 0
	);
	bool parseDegradedReqHeader(
		struct DegradedReqHeader &header, uint8_t opcode,
		char *buf = 0, size_t size = 0, size_t offset = 0
	);
	bool parseListStripeKeyHeader(
		struct ListStripeKeyHeader &header,
		char *buf = 0, size_t size = 0, size_t offset = 0
	);
	bool parseDegradedReleaseReqHeader(
		struct DegradedReleaseReqHeader &header,
		char *buf = 0, size_t size = 0, size_t offset = 0
	);
	bool parseDegradedReleaseResHeader(
		struct DegradedReleaseResHeader &header,
		char *buf = 0, size_t size = 0, size_t offset = 0
	);
	////////////////////
	// Reconstruction //
	////////////////////
	// ---------- recovery_protocol.cc ----------
	bool parseReconstructionHeader(
		struct ReconstructionHeader &header, bool isRequest,
		char *buf = 0, size_t size = 0, size_t offset = 0
	);
	//////////
	// Seal //
	//////////
	// ---------- seal_protocol.cc ----------
	bool parseChunkSealHeader(
		struct ChunkSealHeader &header,
		char *buf = 0, size_t size = 0, size_t offset = 0
	);
	bool parseChunkSealHeaderData(
		struct ChunkSealHeaderData &header,
		char *buf = 0, size_t size = 0, size_t offset = 0
	);
	//////////////////////
	// Chunk operations //
	//////////////////////
	// ---------- chunk_protocol.cc ----------
	bool parseChunkHeader(
		struct ChunkHeader &header,
		char *buf = 0, size_t size = 0, size_t offset = 0
	);
	bool parseChunksHeader(
		struct ChunksHeader &header,
		char *buf = 0, size_t size = 0, size_t offset = 0
	);
	bool parseChunkDataHeader(
		struct ChunkDataHeader &header,
		char *buf = 0, size_t size = 0, size_t offset = 0
	);
	bool parseChunkKeyValueHeader(
		struct ChunkKeyValueHeader &header, char *&ptr,
		char *buf = 0, size_t size = 0, size_t offset = 0
	);
	/////////////////////
	// Acknowledgement //
	/////////////////////
	// ---------- ack_protocol.cc ----------
	bool parseAcknowledgementHeader(
		struct AcknowledgementHeader &header,
		char *buf = 0, size_t size = 0, size_t offset = 0
	);
	bool parseParityDeltaAcknowledgementHeader(
		struct ParityDeltaAcknowledgementHeader &header,
		char *buf = 0, size_t size = 0, size_t offset = 0
	);
};

#endif

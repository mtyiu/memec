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
#define PROTO_OPCODE_DEGRADED_LOCK                0X13
#define PROTO_OPCODE_DEGRADED_UNLOCK              0X14

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
#define PROTO_HEARTBEAT_SIZE 9
struct HeartbeatHeader {
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

#define PROTO_KEY_OP_METADATA_SIZE 14
struct KeyOpMetadataHeader {
	uint8_t keySize;
	uint8_t opcode;
	uint32_t listId;
	uint32_t stripeId;
	uint32_t chunkId;
	char *key;
};

#define PROTO_REMAPPING_RECORD_SIZE 4
struct RemappingRecordHeader {
	uint32_t remap;
};

#define PROTO_SLAVE_SYNC_REMAP_PER_SIZE 10
struct SlaveSyncRemapHeader {
	uint8_t keySize;
	uint8_t opcode;
	uint32_t listId;
	uint32_t chunkId;
	char *key;
};

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
#define PROTO_REMAPPING_LOCK_SIZE 14
struct RemappingLockHeader {
	uint32_t listId;
	uint32_t chunkId;
	uint32_t sockfd;
	bool isRemapped;
	uint8_t keySize;
	char *key;
};

#define PROTO_REMAPPING_SET_SIZE 18
struct RemappingSetHeader {
	uint32_t listId;
	uint32_t chunkId;
	uint32_t sockfd;
	bool remapped;
	bool needsForwarding;
	uint8_t keySize;
	uint32_t valueSize; // 3 bytes
	char *key;
	char *value;
};

#define PROTO_REDIRECT_SIZE 8 /* first 2 fields embedded as KeyHeader */
struct RedirectHeader {
	uint8_t keySize;
	char* key;
	uint32_t listId;
	uint32_t chunkId;
};

////////////////////////
// Degraded operation //
////////////////////////
#define PROTO_DEGRADED_LOCK_REQ_SIZE 17
struct DegradedLockReqHeader {
	// Indicate where the reconstructed chunk should be stored
	// using one of the stripe list that the server belongs to
	uint32_t srcListId;
	uint32_t srcChunkId;
	uint32_t dstListId;
	uint32_t dstChunkId;
	uint8_t keySize;
	char *key;
};

// Size
#define PROTO_DEGRADED_LOCK_RES_BASE_SIZE 2
#define PROTO_DEGRADED_LOCK_RES_LOCK_SIZE 21
#define PROTO_DEGRADED_LOCK_RES_REMAP_SIZE 16
#define PROTO_DEGRADED_LOCK_RES_NOT_SIZE 8
// Type
#define PROTO_DEGRADED_LOCK_RES_IS_LOCKED  1   // Return src* and dst* (same as the client request)
#define PROTO_DEGRADED_LOCK_RES_WAS_LOCKED 2   // Return src* and dst* (different from the client request)
#define PROTO_DEGRADED_LOCK_RES_NOT_LOCKED 3   // Return original srcListId and srcChunkId without dst*
#define PROTO_DEGRADED_LOCK_RES_REMAPPED   4   // Return original srcListId and srcChunkId and remapped dstListId and dstChunkId
#define PROTO_DEGRADED_LOCK_RES_NOT_EXIST  5   // Return original srcListId and srcChunkId
struct DegradedLockResHeader {
	uint8_t type;
	uint8_t keySize;
	char *key;
	uint32_t srcListId;
	uint32_t srcStripeId;
	uint32_t srcChunkId;
	uint32_t dstListId;
	uint32_t dstChunkId;
	bool isSealed;
};

#define PROTO_DEGRADED_REQ_BASE_SIZE 13
struct DegradedReqHeader {
	uint32_t listId;
	uint32_t stripeId;
	uint32_t chunkId;
	bool isSealed;
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

///////////////////////////////////////////////////////////////////////////////

class Protocol {
protected:
	uint8_t from, to;

	size_t generateHeader(
		uint8_t magic, uint8_t to, uint8_t opcode,
		uint32_t length, uint32_t id,
		char *sendBuf = 0
	);
	bool parseHeader(
		uint8_t &magic, uint8_t &from, uint8_t &to, uint8_t &opcode,
		uint32_t &length, uint32_t &id,
		char *buf, size_t size
	);
	//////////////
	// Register //
	//////////////
	size_t generateAddressHeader(
		uint8_t magic, uint8_t to, uint8_t opcode, uint32_t id,
		uint32_t addr, uint16_t port
	);
	bool parseAddressHeader(
		size_t offset, uint32_t &addr, uint16_t &port,
		char *buf, size_t size
	);

	//////////////
	// Reconstruction //
	//////////////
	size_t generateSrcDstAddressHeader(
		uint8_t magic, uint8_t to, uint8_t opcode, uint32_t id,
		uint32_t srcAddr, uint16_t srcPort,
		uint32_t dstAddr, uint16_t dstPort
	);

	size_t generatePromoteBackupSlaveHeader(
		uint8_t magic, uint8_t to, uint8_t opcode, uint32_t id,
		uint32_t addr, uint16_t port,
		std::unordered_set<Metadata> &chunks,
		std::unordered_set<Metadata>::iterator &it,
		bool &isCompleted
	);
	size_t generatePromoteBackupSlaveHeader(
		uint8_t magic, uint8_t to, uint8_t opcode, uint32_t id,
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
	size_t generateHeartbeatMessage(
		uint8_t magic, uint8_t to, uint8_t opcode, uint32_t id,
		LOCK_T *sealedLock, std::unordered_set<Metadata> &sealed, uint32_t &sealedCount,
		LOCK_T *opsLock, std::unordered_map<Key, OpMetadata> &ops, uint32_t &opsCount,
		bool &isCompleted
	);
	bool parseHeartbeatHeader(
		size_t offset, uint32_t &sealed, uint32_t &keys, bool isLast,
		char *buf, size_t size
	);
	bool parseMetadataHeader(
		size_t offset, uint32_t &listId, uint32_t &stripeId, uint32_t &chunkId,
		char *buf, size_t size
	);
	bool parseKeyOpMetadataHeader(
		size_t offset, uint8_t &keySize, uint8_t &opcode,
		uint32_t &listId, uint32_t &stripeId, uint32_t &chunkId,
		char *&key, char *buf, size_t size
	);

	size_t generateRemappingRecordMessage(
		uint8_t magic, uint8_t to, uint8_t opcode, uint32_t id,
		LOCK_T *lock, std::unordered_map<Key, RemappingRecord> &remapRecords,
		size_t &remapCount, char *buf = 0
	);
	bool parseRemappingRecordHeader(
		size_t offset, uint32_t &remap,
		char *buf, size_t size
	);
	bool parseSlaveSyncRemapHeader(
		size_t offset, uint8_t &keySize, uint8_t &opcode,
		uint32_t &listId, uint32_t &chunkId, char *&key,
		char *buf, size_t size
	);

	//////////////////////////
	// Load synchronization //
	//////////////////////////
	size_t generateLoadStatsHeader(
		uint8_t magic, uint8_t to, uint32_t id,
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
	size_t generateKeyHeader(
		uint8_t magic, uint8_t to, uint8_t opcode, uint32_t id,
		uint8_t keySize, char *key, char *sendBuf = 0
	);
	bool parseKeyHeader( size_t offset, uint8_t &keySize, char *&key, char *buf, size_t size );

	size_t generateChunkKeyHeader(
		uint8_t magic, uint8_t to, uint8_t opcode, uint32_t id,
		uint32_t listId, uint32_t stripeId, uint32_t chunkId,
		uint8_t keySize, char *key, char *sendBuf = 0
	);
	bool parseChunkKeyHeader(
		size_t offset, uint32_t &listId, uint32_t &stripeId, uint32_t &chunkId,
		uint8_t &keySize, char *&key,
		char *buf, size_t size
	);

	size_t generateChunkKeyValueUpdateHeader(
		uint8_t magic, uint8_t to, uint8_t opcode, uint32_t id,
		uint32_t listId, uint32_t stripeId, uint32_t chunkId,
		uint8_t keySize, char *key, uint32_t valueUpdateOffset, uint32_t valueUpdateSize,
		uint32_t chunkUpdateOffset, char *valueUpdate, char *sendBuf = 0
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
		uint8_t magic, uint8_t to, uint8_t opcode, uint32_t id,
		uint8_t keySize, char *key, uint32_t valueSize, char *value, char *sendBuf = 0
	);
	bool parseKeyValueHeader(
		size_t offset, uint8_t &keySize, char *&key,
		uint32_t &valueSize, char *&value,
		char *buf, size_t size
	);

	size_t generateKeyValueUpdateHeader(
		uint8_t magic, uint8_t to, uint8_t opcode, uint32_t id,
		uint8_t keySize, char *key,
		uint32_t valueUpdateOffset, uint32_t valueUpdateSize, char *valueUpdate = 0
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
		uint8_t magic, uint8_t to, uint8_t opcode, uint32_t id,
		uint32_t listId, uint32_t stripeId, uint32_t chunkId,
		uint32_t offset, uint32_t length, uint32_t updatingChunkId,
		char *delta = 0, char *sendBuf = 0
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
	size_t generateRemappingLockHeader(
		uint8_t magic, uint8_t to, uint8_t opcode, uint32_t id,
		uint32_t listId, uint32_t chunkId, bool isRemapped,
		uint8_t keySize, char *key, uint32_t sockfd = UINT_MAX
	);
	bool parseRemappingLockHeader(
		size_t offset, uint32_t &listId, uint32_t &chunkId,
		bool &isRemapped, uint8_t &keySize, char *&key,
		char *buf, size_t size, uint32_t &sockfd
	);

	size_t generateRemappingSetHeader(
		uint8_t magic, uint8_t to, uint8_t opcode, uint32_t id,
		uint32_t listId, uint32_t chunkId, bool needsForwarding,
		uint8_t keySize, char *key,
		uint32_t valueSize, char *value, char *sendBuf = 0,
		uint32_t sockfd = UINT_MAX, bool remapped = false
	);
	bool parseRemappingSetHeader(
		size_t offset, uint32_t &listId, uint32_t &chunkId,
		bool &needsForwarding, uint8_t &keySize, char *&key,
		uint32_t &valueSize, char *&value,
		char *buf, size_t size,
		uint32_t &sockfd, bool &remapped
	);

	size_t generateRedirectHeader(
		uint8_t magic, uint8_t to, uint8_t opcode, uint32_t id,
		uint8_t keySize, char *key,
		uint32_t remappedListId, uint32_t remappedChunkId
	);
	bool parseRedirectHeader(
		size_t offset, uint8_t &keySize, char *&key,
		uint32_t &remappedListId, uint32_t &remappedChunkId,
		char *buf, size_t size
	);

	////////////////////////
	// Degraded operation //
	////////////////////////
	size_t generateDegradedLockReqHeader(
		uint8_t magic, uint8_t to, uint8_t opcode, uint32_t id,
		uint32_t srcListId, uint32_t srcChunkId,
		uint32_t dstListId, uint32_t dstChunkId,
		uint8_t keySize, char *key
	);
	bool parseDegradedLockReqHeader(
		size_t offset,
		uint32_t &srcListId, uint32_t &srcChunkId,
		uint32_t &dstListId, uint32_t &dstChunkId,
		uint8_t &keySize, char *&key,
		char *buf, size_t size
	);

	size_t generateDegradedLockResHeader(
		uint8_t magic, uint8_t to, uint8_t opcode, uint32_t id,
		uint8_t type, uint8_t keySize, char *key, char *&buf
	);
	size_t generateDegradedLockResHeader(
		uint8_t magic, uint8_t to, uint8_t opcode, uint32_t id,
		bool isLocked, bool isSealed, uint8_t keySize, char *key,
		uint32_t srcListId, uint32_t srcStripeId, uint32_t srcChunkId,
		uint32_t dstListId, uint32_t dstChunkId
	);
	size_t generateDegradedLockResHeader(
		uint8_t magic, uint8_t to, uint8_t opcode, uint32_t id,
		bool exist,
		uint8_t keySize, char *key,
		uint32_t listId, uint32_t chunkId
	);
	size_t generateDegradedLockResHeader(
		uint8_t magic, uint8_t to, uint8_t opcode, uint32_t id,
		uint8_t keySize, char *key,
		uint32_t srcListId, uint32_t srcChunkId,
		uint32_t dstListId, uint32_t dstChunkId
	);
	bool parseDegradedLockResHeader(
		size_t offset, uint8_t &type,
		uint8_t &keySize, char *&key,
		char *buf, size_t size
	);
	bool parseDegradedLockResHeader(
		size_t offset,
		uint32_t &srcListId, uint32_t &srcStripeId, uint32_t &srcChunkId,
		uint32_t &dstListId, uint32_t &dstChunkId, bool &isSealed,
		char *buf, size_t size
	);
	bool parseDegradedLockResHeader(
		size_t offset,
		uint32_t &listId, uint32_t &chunkId,
		char *buf, size_t size
	);
	bool parseDegradedLockResHeader(
		size_t offset,
		uint32_t &srcListId, uint32_t &srcChunkId,
		uint32_t &dstListId, uint32_t &dstChunkId,
		char *buf, size_t size
	);

	size_t generateDegradedReqHeader(
		uint8_t magic, uint8_t to, uint8_t opcode, uint32_t id,
		uint32_t listId, uint32_t stripeId, uint32_t chunkId, bool isSealed,
		uint8_t keySize, char *key
	);
	size_t generateDegradedReqHeader(
		uint8_t magic, uint8_t to, uint8_t opcode, uint32_t id,
		uint32_t listId, uint32_t stripeId, uint32_t chunkId, bool isSealed,
		uint8_t keySize, char *key,
		uint32_t valueUpdateOffset, uint32_t valueUpdateSize, char *valueUpdate
	);
	bool parseDegradedReqHeader(
		size_t offset,
		uint32_t &listId, uint32_t &stripeId, uint32_t &chunkId, bool &isSealed,
		char *buf, size_t size
	);

	size_t generateListStripeKeyHeader(
		uint8_t magic, uint8_t to, uint8_t opcode, uint32_t id,
		uint32_t listId, uint32_t chunkId, uint8_t keySize, char *key
	);
	bool parseListStripeKeyHeader(
		size_t offset,
		uint32_t &listId, uint32_t &chunkId, uint8_t &keySize, char *&key,
		char *buf, size_t size
	);

	size_t generateDegradedReleaseReqHeader(
		uint8_t magic, uint8_t to, uint8_t opcode, uint32_t id,
		std::vector<Metadata> &chunks,
		bool &isCompleted
	);
    #define parseDegradedReleaseReqHeader parseChunkHeader

	size_t generateDegradedReleaseResHeader(
		uint8_t magic, uint8_t to, uint8_t opcode, uint32_t id,
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
	size_t generateReconstructionHeader(
		uint8_t magic, uint8_t to, uint8_t opcode, uint32_t id,
		uint32_t listId, uint32_t chunkId,
		std::unordered_set<uint32_t> &stripeIds,
		std::unordered_set<uint32_t>::iterator &it,
		uint32_t numChunks,
		bool &isCompleted
	);
	size_t generateReconstructionHeader(
		uint8_t magic, uint8_t to, uint8_t opcode, uint32_t id,
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
	size_t generateChunkSealHeader(
		uint8_t magic, uint8_t to, uint8_t opcode, uint32_t id,
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
	size_t generateChunkHeader(
		uint8_t magic, uint8_t to, uint8_t opcode, uint32_t id,
		uint32_t listId, uint32_t stripeId, uint32_t chunkId, char *sendBuf = 0
	);
	bool parseChunkHeader(
		size_t offset, uint32_t &listId, uint32_t &stripeId, uint32_t &chunkId,
		char *buf, size_t size
	);

	size_t generateChunksHeader(
		uint8_t magic, uint8_t to, uint8_t opcode, uint32_t id,
		uint32_t listId, uint32_t chunkId, std::vector<uint32_t> &stripeIds,
		uint32_t &count
	);
	bool parseChunksHeader(
		size_t offset, uint32_t &listId, uint32_t &chunkId,
		uint32_t &numStripes, uint32_t *&stripeIds,
		char *buf, size_t size
	);

	size_t generateChunkDataHeader(
		uint8_t magic, uint8_t to, uint8_t opcode, uint32_t id,
		uint32_t listId, uint32_t stripeId, uint32_t chunkId,
		uint32_t chunkSize, uint32_t chunkOffset, char *chunkData
	);
	bool parseChunkDataHeader(
		size_t offset, uint32_t &listId, uint32_t &stripeId, uint32_t &chunkId,
		uint32_t &chunkSize, uint32_t &chunkOffset, char *&chunkData,
		char *buf, size_t size
	);

	size_t generateChunkKeyValueHeader(
		uint8_t magic, uint8_t to, uint8_t opcode, uint32_t id,
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

public:
	struct {
		size_t size;
		char *send;
		char *recv;
	} buffer;

	static size_t getSuggestedBufferSize( uint32_t keySize, uint32_t chunkSize );

	Protocol( Role role );
	bool init( size_t size = 0 );
	void free();

	bool parseHeader(
		struct ProtocolHeader &header,
		char *buf = 0, size_t size = 0
	);

	//////////////
	// Register //
	//////////////
	bool parseAddressHeader(
		struct AddressHeader &header,
		char *buf = 0, size_t size = 0, size_t offset = 0
	);
	bool parseSrcDstAddressHeader(
		struct AddressHeader &srcHeader,
		struct AddressHeader &dstHeader,
		char *buf = 0, size_t size = 0, size_t offset = 0
	);
	//////////////
	// Reconstruction //
	//////////////
	bool parsePromoteBackupSlaveHeader(
		struct PromoteBackupSlaveHeader &header, bool isRequest,
		char *buf = 0, size_t size = 0, size_t offset = 0
	);
	//////////////////////////////////////////
	// Heartbeat & metadata synchronization //
	//////////////////////////////////////////
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
	bool parseRemappingRecordHeader(
		struct RemappingRecordHeader &header,
		char *buf = 0, size_t size = 0, size_t offset = 0
	);
	bool parseSlaveSyncRemapHeader(
		struct SlaveSyncRemapHeader &header,
		size_t &bytes, char *buf = 0, size_t size = 0,
		size_t offset = PROTO_HEADER_SIZE + PROTO_REMAPPING_RECORD_SIZE
	);
	//////////////////////////
	// Load synchronization //
	//////////////////////////
	bool parseLoadStatsHeader(
		struct LoadStatsHeader &header,
		char *buf = 0, size_t size = 0, size_t offset = 0
	);
	///////////////////////
	// Normal operations //
	///////////////////////
	bool parseKeyHeader(
		struct KeyHeader &header,
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
	bool parseRemappingLockHeader(
		struct RemappingLockHeader &header,
		char *buf = 0, size_t size = 0, size_t offset = 0
	);
	bool parseRemappingSetHeader(
		struct RemappingSetHeader &header,
		char *buf = 0, size_t size = 0, size_t offset = 0
	);
	bool parseRedirectHeader(
		struct RedirectHeader &header,
		char *buf = 0, size_t size = 0, size_t offset = 0
	);
	////////////////////////
	// Degraded operation //
	////////////////////////
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
	//////////////
	// Reconstruction //
	//////////////
	bool parseReconstructionHeader(
		struct ReconstructionHeader &header, bool isRequest,
		char *buf = 0, size_t size = 0, size_t offset = 0
	);
	//////////
	// Seal //
	//////////
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
};

#endif

#ifndef __COMMON_PROTOCOL_HEADER_HH__
#define __COMMON_PROTOCOL_HEADER_HH__

#define PROTO_HEADER_SIZE 16
struct ProtocolHeader {
	uint8_t magic, from, to; // 1 byte
	bool isLarge;
	uint8_t opcode;          // 1 byte
	uint32_t length;         // 4 bytes
	uint16_t instanceId;     // 2 bytes
	uint32_t requestId;      // 4 bytes
	uint32_t timestamp;      // 4 bytes
};

//////////////
// Register //
//////////////
#define PROTO_NAMED_PIPE_SIZE 1
struct NamedPipeHeader {
	uint8_t length;
	char *pathname;
};

#define PROTO_ADDRESS_SIZE 6
struct AddressHeader {
	uint32_t addr;
	uint16_t port;
};

//////////////
// Recovery //
//////////////
#define PROTO_PROMOTE_BACKUP_SERVER_SIZE 14
struct PromoteBackupServerHeader {
	uint32_t addr;
	uint16_t port;
	uint32_t chunkCount;
	uint32_t unsealedCount;
	uint32_t *metadata;
	char *keys;
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

#define PROTO_KEY_OP_METADATA_SIZE 19
struct KeyOpMetadataHeader {
	uint8_t keySize;
	uint8_t opcode;
	uint32_t listId;
	uint32_t stripeId;
	uint32_t chunkId;
	uint32_t timestamp;
	bool isLarge;
	char *key;
};

#define PROTO_SERVER_REMAPPED_PARITY_SIZE PROTO_ADDRESS_SIZE

//////////////////////////
// Load synchronization //
//////////////////////////
#define PROTO_LOAD_STATS_SIZE 12
struct LoadStatsHeader {
	uint32_t serverGetCount;
	uint32_t serverSetCount;
	uint32_t serverOverloadCount;
};

///////////////////////
// Normal operations //
///////////////////////
#define PROTO_KEY_SIZE 1
struct KeyHeader {
	uint8_t keySize;
	char *key;
};

#define PROTO_KEY_BACKUP_BASE_SIZE       3
#define PROTO_KEY_BACKUP_FOR_DATA_SIZE   17
#define PROTO_KEY_BACKUP_SEALED_SIZE     12
struct KeyBackupHeader {
    bool isParity;
	uint8_t keySize;
	bool isLarge;
    uint32_t timestamp;      // Only for data servers
    uint32_t listId;         // Only for data servers
    uint32_t stripeId;       // Only for data servers
    uint32_t chunkId;        // Only for data servers
	uint8_t sealedCount;     // Only for data servers
	Metadata sealed[ 2 ];    // Only for data servers && isSealed
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

#define PROTO_CHUNK_KEY_VALUE_UPDATE_SIZE 23
struct ChunkKeyValueUpdateHeader {
	uint32_t listId;
	uint32_t stripeId;
	uint32_t chunkId;
	uint8_t keySize;
	uint32_t valueUpdateSize;   // 3 bytes
	uint32_t valueUpdateOffset; // 3 bytes
	uint32_t chunkUpdateOffset; // 3 bytes
	bool isLarge;
	char *key;
	char *valueUpdate;
};

#define PROTO_KEY_VALUE_SIZE 4
#define PROTO_SPLIT_OFFSET_SIZE 3
struct KeyValueHeader {
	uint8_t keySize;
	uint32_t valueSize; // 3 bytes
	char *key;
	uint32_t splitOffset; // 3 bytes (only exists if total object size > chunkSize )
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

//////////////////////
// Batch operations //
//////////////////////
#define PROTO_BATCH_CHUNK_SIZE 4
struct BatchChunkHeader {
	uint32_t count;
	char *chunks; // Array of (request ID + ChunkHeader)
};

#define PROTO_BATCH_KEY_SIZE 4
struct BatchKeyHeader {
	uint32_t count;
	char *keys;
};

#define PROTO_BATCH_KEY_VALUE_SIZE 4
struct BatchKeyValueHeader {
	uint32_t count;
	char *keyValues;
};

///////////////
// Remapping //
///////////////
#define PROTO_REMAPPING_LOCK_SIZE 6
struct RemappingLockHeader {
	uint32_t remappedCount;
	uint8_t keySize;
	bool isLarge;
	char *key;
	uint32_t *original;
	uint32_t *remapped;
};

#define PROTO_DEGRADED_SET_SIZE 17
struct DegradedSetHeader {
	uint32_t listId;
	uint32_t chunkId;
	uint32_t remappedCount;
	uint8_t keySize;
	uint32_t valueSize;   // 3 bytes
	bool isLarge;
	char *key;
	uint32_t splitOffset; // 3 bytes
	char *value;
	uint32_t *original;
	uint32_t *remapped;
};

////////////////////////
// Degraded operation //
////////////////////////
#define PROTO_DEGRADED_LOCK_REQ_SIZE 6
struct DegradedLockReqHeader {
	uint32_t reconstructedCount;
	uint8_t keySize;
	bool isLarge;
	char *key;
	uint32_t *original;
	uint32_t *reconstructed;
};

// Size
#define PROTO_DEGRADED_LOCK_RES_BASE_SIZE   3 // type: 1, 2, 3, 4, 5
#define PROTO_DEGRADED_LOCK_RES_LOCK_SIZE   14 // type: 1, 2
#define PROTO_DEGRADED_LOCK_RES_REMAP_SIZE  4 // type: 4
#define PROTO_DEGRADED_LOCK_RES_NOT_SIZE    0 // type: 3, 5
// Type
#define PROTO_DEGRADED_LOCK_RES_IS_LOCKED   1
#define PROTO_DEGRADED_LOCK_RES_WAS_LOCKED  2
#define PROTO_DEGRADED_LOCK_RES_NOT_LOCKED  3
#define PROTO_DEGRADED_LOCK_RES_REMAPPED    4
#define PROTO_DEGRADED_LOCK_RES_NOT_EXIST   5
// Constant
#define PROTO_DEGRADED_LOCK_NO_ONGOING      -1
struct DegradedLockResHeader {
	uint8_t type;                  // type: 1, 2, 3, 4, 5
	uint8_t keySize;               // type: 1, 2, 3, 4, 5
	bool isLarge;                  // type: 1, 2, 3, 4, 5
	char *key;                     // type: 1, 2, 3, 4, 5

	bool isSealed;                 // type: 1, 2
	uint32_t stripeId;             // type: 1, 2
	uint32_t reconstructedCount;   // type: 1, 2
	uint32_t ongoingAtChunk;       // type: 1, 2; this represents the server that is performing reconstruction on the parity chunks (if any; represented by its chunk ID), set as -1 if there are no such servers
	uint8_t numSurvivingChunkIds;  // type: 1, 2
	uint32_t *survivingChunkIds;   // type: 1, 2
	uint32_t remappedCount;        // type: 4
	uint32_t *original;            // type: 1, 2, 4
	uint32_t *reconstructed;       // type: 1, 2
	uint32_t *remapped;            // type: 4
};

#define PROTO_DEGRADED_REQ_BASE_SIZE 15
struct DegradedReqHeader {
	bool isLarge;
	bool isSealed;
	uint32_t stripeId;
	uint32_t reconstructedCount;
	uint32_t *original;
	uint32_t *reconstructed;
	uint32_t ongoingAtChunk;
	uint8_t numSurvivingChunkIds;
	uint32_t *survivingChunkIds;
	union {
		struct KeyHeader key;
		struct KeyValueUpdateHeader keyValueUpdate;
	} data;
};

#define PROTO_FORWARD_KEY_BASE_SIZE 17
#define PROTO_FORWARD_KEY_UPDATE_SIZE 6
struct ForwardKeyHeader {
	uint8_t opcode;
	uint32_t listId;
	uint32_t stripeId;
	uint32_t chunkId;
	uint8_t keySize;
	uint32_t valueSize; // 3 bytes
	char *key;
	char *value;

	// Only for DEGRADED_UPDATE
	uint32_t valueUpdateSize;   // 3 bytes
	uint32_t valueUpdateOffset; // 3 bytes
	char *valueUpdate;
};

// For retrieving key-value pair in unsealed chunks from parity server
#define PROTO_LIST_STRIPE_KEY_SIZE 9
struct ListStripeKeyHeader {
	uint32_t listId;
	uint32_t chunkId;
	uint8_t keySize;
	char *key;
};

// For asking servers to send back the modified reconstructed chunks to the overloaded server
#define PROTO_DEGRADED_RELEASE_REQ_SIZE 12
struct DegradedReleaseReqHeader {
	uint32_t listId;
	uint32_t stripeId;
	uint32_t chunkId;
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
#define PROTO_CHUNK_SEAL_DATA_SIZE 6
struct ChunkSealHeader {
	uint32_t listId;
	uint32_t stripeId;
	uint32_t chunkId;
	uint32_t count;
};
struct ChunkSealHeaderData {
	uint8_t keySize;
	uint32_t offset;
	bool isLarge;
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

#define PROTO_CHUNK_DATA_SIZE 21
struct ChunkDataHeader {
	uint8_t sealIndicatorCount;
	uint32_t listId;
	uint32_t stripeId;
	uint32_t chunkId;
	uint32_t size;
	uint32_t offset;
	bool *sealIndicator;
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

#define PROTO_ACK_DELTA_SIZE 10
struct DeltaAcknowledgementHeader {
	uint32_t tsCount;         // number of timestamps
	uint32_t keyCount;       // number of requests ids
	uint16_t targetId;      // source data server
};

#endif

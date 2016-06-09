#ifndef __COMMON_PROTOCOL_PROTOCOL_HH__
#define __COMMON_PROTOCOL_PROTOCOL_HH__

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

///////////////////////////////////////////////////////////////////////////////

/**************************************
 * Packet format:
 * - Magic byte (1 byte)
 * - Opcode (1 byte)
 * - Message length (4 bytes)
 * - Message ID (4 bytes)
 * - Instance ID (2 bytes)
 * - Request ID (4 bytes)
 * - timestamp (4 bytes)
 * - Header (variable size)
 ***************************************/
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

#define PROTO_DEGRADED_SET_SIZE 16
struct DegradedSetHeader {
	uint32_t listId;
	uint32_t chunkId;
	uint32_t remappedCount;
	uint8_t keySize;
	uint32_t valueSize;   // 3 bytes
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
>>>>>>> master

#include "magic.hh"
#include "opcode.hh"
#include "header.hh"

///////////////////////////////////////////////////////////////////////////////

#include <endian.h>

class ProtocolUtil {
public:
	static inline size_t write( char *&dst, char *src, uint32_t len ) {
		memmove( dst, src, len );
		dst += len;
		return len;
	}

	static inline size_t write1Byte( char *&dst, char src ) {
		dst[ 0 ] = src;
		dst += 1;
		return 1;
	}

	static inline size_t write2Bytes( char *&dst, uint16_t src, bool toNetworkOrder = true ) {
		*( ( uint16_t * ) dst ) = toNetworkOrder ? htons( src ) : src;
		dst += 2;
		return 2;
	}

	static inline size_t write3Bytes( char *&dst, uint32_t src, bool toNetworkOrder = true ) {
		unsigned char *tmp = ( unsigned char * ) &src;
		if ( toNetworkOrder ) src = htonl( src );
		for ( int i = 0; i < 3; i++ )
			dst[ i ] = tmp[ i + 1 ];
		dst += 3;
		return 3;
	}

	static inline size_t write4Bytes( char *&dst, uint32_t src, bool toNetworkOrder = true ) {
		*( ( uint32_t * ) dst ) = toNetworkOrder ? htonl( src ) : src;
		dst += 4;
		return 4;
	}

	static inline size_t write5Bytes( char *&dst, uint64_t src, bool toNetworkOrder = true ) {
		unsigned char *tmp = ( unsigned char * ) &src;
		if ( toNetworkOrder ) src = htobe64( src );
		for ( int i = 0; i < 5; i++ )
			dst[ i ] = tmp[ i + 3 ];
		dst += 5;
		return 5;
	}

	static inline size_t write6Bytes( char *&dst, uint64_t src, bool toNetworkOrder = true ) {
		unsigned char *tmp = ( unsigned char * ) &src;
		if ( toNetworkOrder ) src = htobe64( src );
		for ( int i = 0; i < 6; i++ )
			dst[ i ] = tmp[ i + 2 ];
		dst += 6;
		return 6;
	}

	static inline size_t write7Bytes( char *&dst, uint64_t src, bool toNetworkOrder = true ) {
		unsigned char *tmp = ( unsigned char * ) &src;
		if ( toNetworkOrder ) src = htobe64( src );
		for ( int i = 0; i < 7; i++ )
			dst[ i ] = tmp[ i + 1 ];
		dst += 7;
		return 7;
	}

	static inline size_t write8Bytes( char *&dst, uint64_t src, bool toNetworkOrder = true ) {
		*( ( uint64_t * ) dst ) = toNetworkOrder ? htobe64( src ) : src;
		dst += 8;
		return 8;
	}

	static inline char *read( char *&src, uint32_t len, char *dst ) {
		memmove( dst, src, len );
		src += len;
		return dst;
	}

	static inline char read1Byte( char *&src ) {
		char ret = src[ 0 ];
		src += 1;
		return ret;
	}

	static inline uint16_t read2Bytes( char *&src, bool toHostOrder = true ) {
		uint16_t ret = *( ( uint16_t * ) src );
		src += 2;
		return toHostOrder ? ntohs( ret ) : ret;
	}

	static inline uint32_t read3Bytes( char *&src, bool toHostOrder = true ) {
		uint32_t ret = 0;
		unsigned char *tmp = ( unsigned char * ) &ret;
		for ( int i = 0; i < 3; i++ )
			tmp[ i + 1 ] = src[ i ];
		src += 3;
		return toHostOrder ? ntohl( ret ) : ret;
	}

	static inline uint32_t read4Bytes( char *&src, bool toHostOrder = true ) {
		uint32_t ret = *( ( uint32_t * ) src );
		src += 4;
		return toHostOrder ? ntohl( ret ) : ret;
	}

	static inline uint64_t read5Bytes( char *&src, bool toHostOrder = true ) {
		uint64_t ret = 0;
		unsigned char *tmp = ( unsigned char * ) &ret;
		for ( int i = 0; i < 5; i++ )
			tmp[ i + 3 ] = src[ i ];
		src += 5;
		return toHostOrder ? be64toh( ret ) : ret;
	}

	static inline uint64_t read6Bytes( char *&src, bool toHostOrder = true ) {
		uint64_t ret = 0;
		unsigned char *tmp = ( unsigned char * ) &ret;
		for ( int i = 0; i < 6; i++ )
			tmp[ i + 2 ] = src[ i ];
		src += 6;
		return toHostOrder ? be64toh( ret ) : ret;
	}

	static inline uint64_t read7Bytes( char *&src, bool toHostOrder = true ) {
		uint64_t ret = 0;
		unsigned char *tmp = ( unsigned char * ) &ret;
		for ( int i = 0; i < 7; i++ )
			tmp[ i + 1 ] = src[ i ];
		src += 7;
		return toHostOrder ? be64toh( ret ) : ret;
	}

	static inline uint64_t read8Bytes( char *&src, bool toHostOrder = true ) {
		uint64_t ret = *( ( uint64_t * ) src );
		src += 8;
		return toHostOrder ? be64toh( ret ) : ret;
	}
};

///////////////////////////////////////////////////////////////////////////////

enum Role {
	ROLE_APPLICATION,
	ROLE_COORDINATOR,
	ROLE_CLIENT,
	ROLE_SERVER
};

class Protocol {
protected:
	uint8_t from, to;
	Timestamp localTimestamp;

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
	size_t generateHeader(
		uint8_t magic, uint8_t to, uint8_t opcode,
		uint32_t length, uint16_t instanceId, uint32_t requestId,
		char *sendBuf = 0, uint32_t requestTimestamp = 0
	);
	bool parseHeader( struct ProtocolHeader &header, char *buf = 0, size_t size = 0 );
	static size_t getSuggestedBufferSize( uint32_t keySize, uint32_t chunkSize );

	//////////////
	// Register //
	//////////////
	// ---------- address_protocol.cc ----------
	size_t generateAddressHeader(
		uint8_t magic, uint8_t to, uint8_t opcode, uint16_t instanceId, uint32_t requestId,
		uint32_t addr, uint16_t port, char* buf = 0
	);
	bool parseAddressHeader(
		struct AddressHeader &header,
		char *buf = 0, size_t size = 0, size_t offset = 0
	);

	size_t generateSrcDstAddressHeader(
		uint8_t magic, uint8_t to, uint8_t opcode, uint16_t instanceId, uint32_t requestId,
		uint32_t srcAddr, uint16_t srcPort,
		uint32_t dstAddr, uint16_t dstPort
	);
	bool parseSrcDstAddressHeader(
		struct AddressHeader &srcHeader,
		struct AddressHeader &dstHeader,
		char *buf = 0, size_t size = 0, size_t offset = 0
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
	size_t generateMetadataBackupMessage(
		uint8_t magic, uint8_t to, uint8_t opcode,
		uint16_t instanceId, uint32_t requestId,
		uint32_t addr, uint16_t port,
		LOCK_T *lock,
		std::unordered_multimap<uint32_t, Metadata> &sealed, uint32_t &sealedCount,
		std::unordered_map<Key, MetadataBackup> &ops, uint32_t &opsCount,
		bool &isCompleted
	);
	bool parseMetadataBackupMessage(
		struct AddressHeader &address,
		struct HeartbeatHeader &heartbeat,
		char *buf = 0, size_t size = 0, size_t offset = 0
	);

	//////////////////////////
	// Load synchronization //
	//////////////////////////
	// ---------- load_protocol.cc ----------
	size_t generateLoadStatsHeader(
		uint8_t magic, uint8_t to, uint16_t instanceId, uint32_t requestId,
		uint32_t serverGetCount, uint32_t serverSetCount, uint32_t serverOverloadCount,
		uint32_t recordSize, uint32_t serverAddrSize
	);
	bool parseLoadStatsHeader(
		struct LoadStatsHeader &header,
		char *buf = 0, size_t size = 0, size_t offset = 0
	);

	///////////////////////
	// Normal operations //
	///////////////////////
	// ---------- normal_protocol.cc ----------
	size_t generateKeyHeader(
		uint8_t magic, uint8_t to, uint8_t opcode, uint16_t instanceId, uint32_t requestId,
		uint8_t keySize, char *key, char *sendBuf = 0, uint32_t timestamp = 0
	);
	bool parseKeyHeader(
		struct KeyHeader &header,
		char *buf = 0, size_t size = 0, size_t offset = 0
	);

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
		struct KeyBackupHeader &header,
		char *buf = 0, size_t size = 0, size_t offset = 0
	);

	size_t generateChunkKeyHeader(
		uint8_t magic, uint8_t to, uint8_t opcode, uint16_t instanceId, uint32_t requestId,
		uint32_t listId, uint32_t stripeId, uint32_t chunkId,
		uint8_t keySize, char *key, char *sendBuf = 0, uint32_t timestamp = 0
	);
	bool parseChunkKeyHeader(
		struct ChunkKeyHeader &header,
		char *buf = 0, size_t size = 0, size_t offset = 0
	);

	size_t generateChunkKeyValueUpdateHeader(
		uint8_t magic, uint8_t to, uint8_t opcode, uint16_t instanceId, uint32_t requestId,
		uint32_t listId, uint32_t stripeId, uint32_t chunkId,
		uint8_t keySize, bool isLarge, char *key, uint32_t valueUpdateOffset, uint32_t valueUpdateSize,
		uint32_t chunkUpdateOffset, char *valueUpdate, char *sendBuf = 0,
		uint32_t timestamp = 0
	);
	bool parseChunkKeyValueUpdateHeader(
		struct ChunkKeyValueUpdateHeader &header, bool withValueUpdate,
		char *buf = 0, size_t size = 0, size_t offset = 0
	);

	size_t generateKeyValueHeader(
		uint8_t magic, uint8_t to, uint8_t opcode, uint16_t instanceId, uint32_t requestId,
		uint8_t keySize, char *key, uint32_t valueSize, char *value, char *sendBuf = 0,
		uint32_t timestamp = 0,
        uint32_t splitOffset = 0, uint32_t splitSize = 0
	);
	bool parseKeyValueHeader(
		struct KeyValueHeader &header,
		char *buf = 0, size_t size = 0, size_t offset = 0
	);

	size_t generateKeyValueUpdateHeader(
		uint8_t magic, uint8_t to, uint8_t opcode, uint16_t instanceId, uint32_t requestId,
		uint8_t keySize, char *key,
		uint32_t valueUpdateOffset, uint32_t valueUpdateSize, char *valueUpdate = 0,
		char *sendBuf = 0,
		uint32_t timestamp = 0
	);
	bool parseKeyValueUpdateHeader(
		struct KeyValueUpdateHeader &header, bool withValueUpdate,
		char *buf = 0, size_t size = 0, size_t offset = 0
	);

	size_t generateChunkUpdateHeader(
		uint8_t magic, uint8_t to, uint8_t opcode, uint16_t instanceId, uint32_t requestId,
		uint32_t listId, uint32_t stripeId, uint32_t chunkId,
		uint32_t offset, uint32_t length, uint32_t updatingChunkId,
		char *delta = 0, char *sendBuf = 0, uint32_t timestamp = 0
	);
	bool parseChunkUpdateHeader(
		struct ChunkUpdateHeader &header, bool withDelta,
		char *buf = 0, size_t size = 0, size_t offset = 0
	);

	//////////////////////
	// Batch operations //
	//////////////////////
	// ---------- batch_protocol.cc ----------
	size_t generateBatchChunkHeader(
		uint8_t magic, uint8_t to, uint8_t opcode,
		uint16_t instanceId, uint32_t requestId,
		std::vector<uint32_t> *requestIds,
		std::vector<Metadata> *metadata,
		uint32_t &chunksCount,
		bool &isCompleted
	);
	bool parseBatchChunkHeader(
		struct BatchChunkHeader &header,
		char *buf = 0, size_t size = 0, size_t offset = 0
	);
	bool nextChunkInBatchChunkHeader(
		struct BatchChunkHeader &header,
		uint32_t &responseId,
		struct ChunkHeader &chunkHeader,
		uint32_t size, uint32_t &offset
	);

	size_t generateBatchKeyHeader(
		uint8_t magic, uint8_t to, uint8_t opcode,
		uint16_t instanceId, uint32_t requestId,
		std::unordered_set<Key> &keys, std::unordered_set<Key>::iterator &it, uint32_t &keysCount,
		bool &isCompleted
	);
	size_t generateBatchKeyHeader(
		uint8_t magic, uint8_t to, uint8_t opcode,
		uint16_t instanceId, uint32_t requestId,
		struct BatchKeyValueHeader &header
	);
	bool parseBatchKeyHeader(
		struct BatchKeyHeader &header,
		char *buf = 0, size_t size = 0, size_t offset = 0
	);
	void nextKeyInBatchKeyHeader(
		struct BatchKeyHeader &header,
		uint8_t &keySize, char *&key, uint32_t &offset
	);

	size_t generateBatchKeyValueHeader(
		uint8_t magic, uint8_t to, uint8_t opcode,
		uint16_t instanceId, uint32_t requestId,
		std::unordered_set<Key> &keys, std::unordered_set<Key>::iterator &it,
		std::unordered_map<Key, KeyValue> *values, LOCK_T *lock,
		uint32_t &keyValuesCount,
		bool &isCompleted
	);
	bool parseBatchKeyValueHeader(
		struct BatchKeyValueHeader &header,
		char *buf = 0, size_t size = 0, size_t offset = 0
	);
	void nextKeyValueInBatchKeyValueHeader(
		struct BatchKeyValueHeader &header,
		uint8_t &keySize, uint32_t &valueSize, char *&key, char *&value,
		uint32_t &offset
	);
	///////////////
	// Remapping //
	///////////////
	// ---------- remap_protocol.cc ----------
	size_t generateRemappingLockHeader(
		uint8_t magic, uint8_t to, uint8_t opcode, uint16_t instanceId, uint32_t requestId,
		uint32_t *original, uint32_t *remapped, uint32_t remappedCount,
		uint8_t keySize, char *key, bool isLarge
	);
	bool parseRemappingLockHeader(
		struct RemappingLockHeader &header,
		char *buf = 0, size_t size = 0, size_t offset = 0
	);

	size_t generateDegradedSetHeader(
		uint8_t magic, uint8_t to, uint8_t opcode, uint16_t instanceId, uint32_t requestId,
		uint32_t listId, uint32_t chunkId,
		uint32_t *original, uint32_t *remapped, uint32_t remappedCount,
		uint8_t keySize, char *key,
		uint32_t valueSize, char *value,
        uint32_t splitOffset = 0, uint32_t splitSize = 0,
		char *sendBuf = 0
	);
	bool parseDegradedSetHeader(
		struct DegradedSetHeader &header,
		char *buf = 0, size_t size = 0, size_t offset = 0,
		struct sockaddr_in *target = 0
	);

	////////////////////////
	// Degraded operation //
	////////////////////////
	// ---------- degraded_protocol.cc ----------
	size_t generateDegradedLockReqHeader(
		uint8_t magic, uint8_t to, uint8_t opcode, uint16_t instanceId, uint32_t requestId,
		uint32_t *original, uint32_t *reconstructed, uint32_t reconstructedCount,
		uint8_t keySize, char *key, bool isLarge
	);
	bool parseDegradedLockReqHeader(
		struct DegradedLockReqHeader &header,
		char *buf = 0, size_t size = 0, size_t offset = 0
	);

	size_t generateDegradedLockResHeader(
		uint8_t magic, uint8_t to, uint8_t opcode, uint16_t instanceId, uint32_t requestId,
		uint32_t length, uint8_t type, uint8_t keySize, char *key, bool isLarge, char *&buf
	);
	size_t generateDegradedLockResHeader(
		uint8_t magic, uint8_t to, uint8_t opcode, uint16_t instanceId, uint32_t requestId,
		bool isLocked, uint8_t keySize, char *key, bool isLarge,
		bool isSealed, uint32_t stripeId, uint32_t dataChunkId, uint32_t dataChunkCount,
		uint32_t *original, uint32_t *reconstructed, uint32_t reconstructedCount,
		uint32_t ongoingAtChunk,
		uint8_t numSurvivingChunkIds, uint32_t *survivingChunkIds
	);
	size_t generateDegradedLockResHeader(
		uint8_t magic, uint8_t to, uint8_t opcode, uint16_t instanceId, uint32_t requestId,
		bool exist, uint8_t keySize, char *key, bool isLarge
	);
	size_t generateDegradedLockResHeader(
		uint8_t magic, uint8_t to, uint8_t opcode, uint16_t instanceId, uint32_t requestId,
		uint8_t keySize, char *key, bool isLarge,
		uint32_t *original, uint32_t *remapped, uint32_t remappedCount
	);
	bool parseDegradedLockResHeader(
		size_t offset, uint8_t &type,
		uint8_t &keySize, char *&key, bool &isLarge,
		char *buf, size_t size
	);
	bool parseDegradedLockResHeader(
		size_t offset, bool &isSealed,
		uint32_t &stripeId,
		uint32_t *&original, uint32_t *&reconstructed, uint32_t &reconstructedCount,
		uint32_t &ongoingAtChunk,
		uint8_t &numSurvivingChunkIds, uint32_t *&survivingChunkIds,
		char *buf, size_t size
	);
	bool parseDegradedLockResHeader(
		size_t offset,
		uint32_t *&original, uint32_t *&remapped, uint32_t &remappedCount,
		char *buf, size_t size
	);
	bool parseDegradedLockResHeader(
		struct DegradedLockResHeader &header,
		char *buf = 0, size_t size = 0, size_t offset = 0
	);

	size_t generateDegradedReqHeader(
		uint8_t magic, uint8_t to, uint8_t opcode, uint16_t instanceId, uint32_t requestId,
		bool isSealed, uint32_t stripeId,
		uint32_t *original, uint32_t *reconstructed, uint32_t reconstructedCount,
		uint32_t ongoingAtChunk, uint8_t numSurvivingChunkIds, uint32_t *survivingChunkIds,
		uint8_t keySize, char *key, bool isLarge,
		uint32_t timestamp = 0
	);
	size_t generateDegradedReqHeader(
		uint8_t magic, uint8_t to, uint8_t opcode, uint16_t instanceId, uint32_t requestId,
		bool isSealed, uint32_t stripeId,
		uint32_t *original, uint32_t *reconstructed, uint32_t reconstructedCount,
		uint32_t ongoingAtChunk, uint8_t numSurvivingChunkIds, uint32_t *survivingChunkIds,
		uint8_t keySize, char *key, bool isLarge,
		uint32_t valueUpdateOffset, uint32_t valueUpdateSize, char *valueUpdate,
		uint32_t timestamp = 0
	);
	bool parseDegradedReqHeader(
		size_t offset,
		bool &isSealed, uint32_t &stripeId, bool &isLarge,
		uint32_t *&original, uint32_t *&reconstructed, uint32_t &reconstructedCount,
		uint32_t &ongoingAtChunk, uint8_t &numSurvivingChunkIds, uint32_t *&survivingChunkIds,
		char *buf, size_t size
	);
	bool parseDegradedReqHeader(
		struct DegradedReqHeader &header, uint8_t opcode,
		char *buf = 0, size_t size = 0, size_t offset = 0
	);

	size_t generateForwardKeyReqHeader(
    	uint8_t magic, uint8_t to, uint8_t opcode,
    	uint16_t instanceId, uint32_t requestId,
    	uint8_t degradedOpcode, uint32_t listId, uint32_t stripeId, uint32_t chunkId,
    	uint8_t keySize, char *key, uint32_t valueSize, char *value,
    	uint32_t valueUpdateSize, uint32_t valueUpdateOffset, char *valueUpdate
    );
    bool parseForwardKeyReqHeader(
        struct ForwardKeyHeader &header,
        char *buf = 0, size_t size = 0, size_t offset = 0
    );

	size_t generateForwardKeyResHeader(
    	uint8_t magic, uint8_t to, uint8_t opcode,
    	uint16_t instanceId, uint32_t requestId,
    	uint8_t degradedOpcode, uint32_t listId, uint32_t stripeId, uint32_t chunkId,
    	uint8_t keySize, char *key, uint32_t valueSize,
    	uint32_t valueUpdateSize, uint32_t valueUpdateOffset
    );
    bool parseForwardKeyResHeader(
        struct ForwardKeyHeader &header,
        char *buf = 0, size_t size = 0, size_t offset = 0
    );

	size_t generateListStripeKeyHeader(
		uint8_t magic, uint8_t to, uint8_t opcode, uint16_t instanceId, uint32_t requestId,
		uint32_t listId, uint32_t chunkId, uint8_t keySize, char *key
	);
	bool parseListStripeKeyHeader(
		struct ListStripeKeyHeader &header,
		char *buf = 0, size_t size = 0, size_t offset = 0
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
		struct DegradedReleaseResHeader &header,
		char *buf = 0, size_t size = 0, size_t offset = 0
	);

	////////////////////
	// Reconstruction //
	////////////////////
	// ---------- recovery_protocol.cc ----------
	size_t generatePromoteBackupServerHeader(
		uint8_t magic, uint8_t to, uint8_t opcode, uint16_t instanceId, uint32_t requestId,
		uint32_t addr, uint16_t port,
		std::unordered_set<Metadata> &chunks,
		std::unordered_set<Metadata>::iterator &chunksIt,
		std::unordered_set<Key> &keys,
		std::unordered_set<Key>::iterator &keysIt,
		bool &isCompleted
	);
	size_t generatePromoteBackupServerHeader(
		uint8_t magic, uint8_t to, uint8_t opcode, uint16_t instanceId, uint32_t requestId,
		uint32_t addr, uint16_t port, uint32_t numReconstructedChunks, uint32_t numReconstructedKeys
	);
	bool parsePromoteBackupServerHeader(
		struct PromoteBackupServerHeader &header, bool isRequest,
		char *buf = 0, size_t size = 0, size_t offset = 0
	);
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
		struct ReconstructionHeader &header, bool isRequest,
		char *buf = 0, size_t size = 0, size_t offset = 0
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
	size_t generateChunkHeader(
		uint8_t magic, uint8_t to, uint8_t opcode, uint16_t instanceId, uint32_t requestId,
		uint32_t listId, uint32_t stripeId, uint32_t chunkId, char *sendBuf = 0
	);
	bool parseChunkHeader(
		struct ChunkHeader &header,
		char *buf = 0, size_t size = 0, size_t offset = 0
	);

	size_t generateChunksHeader(
		uint8_t magic, uint8_t to, uint8_t opcode, uint16_t instanceId, uint32_t requestId,
		uint32_t listId, uint32_t chunkId, std::vector<uint32_t> &stripeIds,
		uint32_t &count
	);
	bool parseChunksHeader(
		struct ChunksHeader &header,
		char *buf = 0, size_t size = 0, size_t offset = 0
	);

	size_t generateChunkDataHeader(
		uint8_t magic, uint8_t to, uint8_t opcode, uint16_t instanceId, uint32_t requestId,
		uint32_t listId, uint32_t stripeId, uint32_t chunkId,
		uint32_t chunkSize, uint32_t chunkOffset, char *chunkData,
		uint8_t sealIndicatorCount, bool *sealIndicator
	);
	bool parseChunkDataHeader(
		struct ChunkDataHeader &header,
		char *buf = 0, size_t size = 0, size_t offset = 0
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
		struct ChunkKeyValueHeader &header, char *&dataPtr,
		char *buf = 0, size_t size = 0, size_t offset = 0
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
		struct AcknowledgementHeader &header,
		char *buf = 0, size_t size = 0, size_t offset = 0
	);

	size_t generateDeltaAcknowledgementHeader(
		uint8_t magic, uint8_t to, uint8_t opcode, uint16_t instanceId, uint32_t requestId,
		const std::vector<uint32_t> &timestamp, const std::vector<Key> &requests,
		uint16_t targetId, char* buf = 0
	);
	bool parseDeltaAcknowledgementHeader(
		struct DeltaAcknowledgementHeader &header,
		std::vector<uint32_t> *timestamps = 0,
		std::vector<Key> *requests = 0,
		char *buf = 0, size_t size = 0, size_t offset = 0
	);
};

#endif

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
 * - Timestamp (4 bytes)
 * - Header (variable size)
 ***************************************/

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
		char *sendBuf = 0, uint32_t requestTimestamp = 0, bool isLarge = false
	);
	bool parseHeader( struct ProtocolHeader &header, char *buf = 0, size_t size = 0 );
	static size_t getSuggestedBufferSize( uint32_t keySize, uint32_t chunkSize, bool supportLargeObject = false );

	//////////////
	// Register //
	//////////////
	// ---------- register_protocol.cc ----------
	size_t generateNamedPipeHeader(
		uint8_t magic, uint8_t to, uint8_t opcode, uint16_t instanceId, uint32_t requestId,
		uint8_t readLength, uint8_t writeLength, char *readPathname, char *writePathname,
		char* buf = 0
	);
	bool parseNamedPipeHeader(
		struct NamedPipeHeader &header,
		char *buf = 0, size_t size = 0, size_t offset = 0
	);

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
		uint8_t keySize, char *key, char *sendBuf = 0, uint32_t timestamp = 0, bool isLarge = false
	);
	bool parseKeyHeader(
		struct KeyHeader &header,
		char *buf = 0, size_t size = 0, size_t offset = 0
	);

	size_t generateKeyBackupHeader(
		uint8_t magic, uint8_t to, uint8_t opcode, uint16_t instanceId, uint32_t requestId,
		uint32_t timestamp,
		uint32_t listId, uint32_t stripeId, uint32_t chunkId,
		Metadata *sealed, uint8_t sealedCount,
		uint8_t keySize, char *key, bool isLarge, char *sendBuf = 0
	);
	size_t generateKeyBackupHeader(
		uint8_t magic, uint8_t to, uint8_t opcode, uint16_t instanceId, uint32_t requestId,
        uint32_t timestamp,
        uint32_t listId, uint32_t stripeId, uint32_t chunkId,
		uint8_t keySize, char *key, bool isLarge, char *sendBuf = 0
	);
    size_t generateKeyBackupHeader(
		uint8_t magic, uint8_t to, uint8_t opcode, uint16_t instanceId, uint32_t requestId,
		uint8_t keySize, char *key, bool isLarge, char *sendBuf = 0
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
        uint32_t splitOffset = 0, uint32_t splitSize = 0, bool isLarge = false
	);
	bool parseKeyValueHeader(
		struct KeyValueHeader &header,
		char *buf = 0, size_t size = 0, size_t offset = 0, bool enableSplit = false, bool isLarge = false
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
		uint32_t listId, uint32_t chunkId, uint8_t keySize, char *key, bool isLarge
	);
	bool parseListStripeKeyHeader(
		struct ListStripeKeyHeader &header, bool isLarge,
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

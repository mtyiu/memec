#ifndef __SLAVE_PROTOCOL_PROTOCOL_HH__
#define __SLAVE_PROTOCOL_PROTOCOL_HH__

#include "../../common/config/server_addr.hh"
#include "../../common/ds/chunk.hh"
#include "../../common/lock/lock.hh"
#include "../../common/protocol/protocol.hh"

class SlaveProtocol : public Protocol {
public:
	SlaveProtocol() : Protocol( ROLE_SLAVE ) {}

	// ---------- register_protocol.cc ----------
	char *reqRegisterCoordinator( size_t &size, uint32_t id, uint32_t addr, uint16_t port );
	char *resRegisterMaster( size_t &size, uint32_t id, bool success );
	char *reqRegisterSlavePeer( size_t &size, uint32_t id, ServerAddr *addr );
	char *resRegisterSlavePeer( size_t &size, uint32_t id, bool success );

	// ---------- heartbeat_protocol.cc ----------
	char *sendHeartbeat(
		size_t &size, uint32_t id, uint32_t timestamp,
		LOCK_T *sealedLock, std::unordered_set<Metadata> &sealed, uint32_t &sealedCount,
		LOCK_T *opsLock, std::unordered_map<Key, OpMetadata> &ops, uint32_t &opsCount,
		bool &isCompleted
	);

	// ---------- normal_master_protocol.cc ----------
	char *resSet(
		size_t &size, uint32_t id,
		uint32_t timestamp, uint32_t listId, uint32_t stripeId, uint32_t chunkId,
		uint8_t keySize, char *key
	);
	char *resSet(
		size_t &size, uint32_t id, bool success,
		uint8_t keySize, char *key, bool toMaster = true
	);
	char *resGet(
		size_t &size, uint32_t id, bool success, bool isDegraded,
		uint8_t keySize, char *key, uint32_t valueSize = 0, char *value = 0,
		bool toMaster = true
	);
	char *resUpdate(
		size_t &size, uint32_t id, bool success, bool isDegraded,
		uint8_t keySize, char *key,
		uint32_t valueUpdateOffset, uint32_t valueUpdateSize
	);
	char *resDelete(
		size_t &size, uint32_t id, bool isDegraded,
		uint32_t timestamp, uint32_t listId, uint32_t stripeId, uint32_t chunkId,
		uint8_t keySize, char *key,
		bool toMaster = true
	);
	char *resDelete(
		size_t &size, uint32_t id, bool isDegraded,
		uint8_t keySize, char *key,
		bool toMaster = true
	);

	// ---------- remap_protocol.cc ----------
	char *resRemappingSet(
		size_t &size, bool toMaster, uint32_t id, bool success,
		uint32_t listId, uint32_t chunkId,
		uint8_t keySize, char *key,
		uint32_t sockfd = UINT_MAX, bool remapped = false
	);
	char *resRemapParity( size_t &size, uint32_t id );

	// ---------- degraded_protocol.cc ----------
	char *resReleaseDegradedLock( size_t &size, uint32_t id, uint32_t count );

	// ---------- seal_protocol.cc ----------
	char *reqSealChunk( size_t &size, uint32_t id, Chunk *chunk, uint32_t startPos, char *buf = 0 );

	// ---------- recovery_protocol.cc ----------
	char *resReconstruction( size_t &size, uint32_t id, uint32_t listId, uint32_t chunkId, uint32_t numStripes );
	char *resPromoteBackupSlave( size_t &size, uint32_t id, uint32_t addr, uint16_t port, uint32_t numStripes );

	// ---------- normal_slave_protocol.cc ----------
	char *reqSet(
		size_t &size, uint32_t id,
		char *key, uint8_t keySize,
		char *value, uint32_t valueSize,
		char *buf = 0
	);

	char *reqGet(
		size_t &size, uint32_t id,
		uint32_t listId, uint32_t chunkId,
		uint8_t keySize, char *key
	);

	char *reqUpdate(
		size_t &size, uint32_t id,
		uint32_t listId, uint32_t stripeId, uint32_t chunkId,
		char *key, uint8_t keySize,
		char *valueUpdate, uint32_t valueUpdateOffset, uint32_t valueUpdateSize,
		uint32_t chunkUpdateOffset, char *buf = 0
	);
	char *resUpdate(
		size_t &size, uint32_t id, bool success,
		uint32_t listId, uint32_t stripeId, uint32_t chunkId,
		char *key, uint8_t keySize, uint32_t valueUpdateOffset, uint32_t valueUpdateSize,
		uint32_t chunkUpdateOffset, char *buf = 0
	);

	char *reqDelete(
		size_t &size, uint32_t id,
		uint32_t listId, uint32_t stripeId, uint32_t chunkId,
		char *key, uint8_t keySize, char *buf = 0
	);
	char *resDelete(
		size_t &size, uint32_t id, bool success,
		uint32_t listId, uint32_t stripeId, uint32_t chunkId,
		char *key, uint8_t keySize, char *buf = 0
	);

	char *reqGetChunk(
		size_t &size, uint32_t id,
		uint32_t listId, uint32_t stripeId, uint32_t chunkId,
		char *buf = 0
	);
	char *resGetChunk(
		size_t &size, uint32_t id, bool success,
		uint32_t listId, uint32_t stripeId, uint32_t chunkId,
		uint32_t chunkSize = 0, uint32_t chunkOffset = 0, char *chunkData = 0
	);

	char *reqSetChunk(
		size_t &size, uint32_t id,
		uint32_t listId, uint32_t stripeId, uint32_t chunkId,
		uint32_t chunkSize, uint32_t chunkOffset, char *chunkData
	);
	char *reqSetChunk(
		size_t &size, uint32_t id,
		uint32_t listId, uint32_t stripeId, uint32_t chunkId,
		std::unordered_map<Key, KeyValue> *values,
		std::unordered_multimap<Metadata, Key> *metadataRev,
		std::unordered_set<Key> *deleted,
		LOCK_T *lock, bool &isCompleted
	);
	char *resSetChunk(
		size_t &size, uint32_t id, bool success,
		uint32_t listId, uint32_t stripeId, uint32_t chunkId
	);

	char *reqUpdateChunk(
		size_t &size, uint32_t id,
		uint32_t listId, uint32_t stripeId, uint32_t chunkId,
		uint32_t offset, uint32_t length, uint32_t updatingChunkId,
		char *delta, char *buf = 0
	);
	char *resUpdateChunk(
		size_t &size, uint32_t id, bool success,
		uint32_t listId, uint32_t stripeId, uint32_t chunkId,
		uint32_t offset, uint32_t length, uint32_t updatingChunkId
	);

	char *reqDeleteChunk(
		size_t &size, uint32_t id,
		uint32_t listId, uint32_t stripeId, uint32_t chunkId,
		uint32_t offset, uint32_t length, uint32_t updatingChunkId,
		char *delta, char *buf = 0
	);
	char *resDeleteChunk(
		size_t &size, uint32_t id, bool success,
		uint32_t listId, uint32_t stripeId, uint32_t chunkId,
		uint32_t offset, uint32_t length, uint32_t updatingChunkId
	);

	// ---------- ack_protocol.cc ----------
	char *ackMetadata( size_t &size, uint32_t id, uint32_t fromTimestamp, uint32_t toTimestamp );
};
#endif

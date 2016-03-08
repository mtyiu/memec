#ifndef __SERVER_PROTOCOL_PROTOCOL_HH__
#define __SERVER_PROTOCOL_PROTOCOL_HH__

#include "../../common/config/server_addr.hh"
#include "../../common/ds/chunk.hh"
#include "../../common/lock/lock.hh"
#include "../../common/protocol/protocol.hh"

class ServerProtocol : public Protocol {
public:
	ServerProtocol() : Protocol( ROLE_SERVER ) {}

	// ---------- register_protocol.cc ----------
	char *reqRegisterCoordinator( size_t &size, uint32_t requestId, uint32_t addr, uint16_t port );
	char *resRegisterMaster( size_t &size, uint16_t instanceId, uint32_t requestId, bool success );
	char *reqRegisterServerPeer( size_t &size, uint16_t instanceId, uint32_t requestId, ServerAddr *addr );
	char *resRegisterServerPeer( size_t &size, uint16_t instanceId, uint32_t requestId, bool success );

	// ---------- heartbeat_protocol.cc ----------
	char *sendHeartbeat(
		size_t &size, uint16_t instanceId, uint32_t requestId, uint32_t timestamp,
		LOCK_T *sealedLock, std::unordered_set<Metadata> &sealed, uint32_t &sealedCount,
		LOCK_T *opsLock, std::unordered_map<Key, OpMetadata> &ops, uint32_t &opsCount,
		bool &isCompleted
	);

	// ---------- normal_client_protocol.cc ----------
	char *resSet(
		size_t &size, uint16_t instanceId, uint32_t requestId,
		uint32_t timestamp, uint32_t listId, uint32_t stripeId, uint32_t chunkId,
		bool isSealed, uint32_t sealedListId, uint32_t sealedStripeId, uint32_t sealedChunkId,
		uint8_t keySize, char *key
	);
	char *resSet(
		size_t &size, uint16_t instanceId, uint32_t requestId, bool success,
		uint8_t keySize, char *key, bool toMaster = true
	);
	char *resGet(
		size_t &size, uint16_t instanceId, uint32_t requestId, bool success, bool isDegraded,
		uint8_t keySize, char *key, uint32_t valueSize = 0, char *value = 0,
		bool toMaster = true
	);
	char *resUpdate(
		size_t &size, uint16_t instanceId, uint32_t requestId, bool success, bool isDegraded,
		uint8_t keySize, char *key,
		uint32_t valueUpdateOffset, uint32_t valueUpdateSize
	);
	char *resDelete(
		size_t &size, uint16_t instanceId, uint32_t requestId, bool isDegraded,
		uint32_t timestamp, uint32_t listId, uint32_t stripeId, uint32_t chunkId,
		uint8_t keySize, char *key,
		bool toMaster = true
	);
	char *resDelete(
		size_t &size, uint16_t instanceId, uint32_t requestId, bool isDegraded,
		uint8_t keySize, char *key,
		bool toMaster = true
	);

	// ---------- remap_protocol.cc ----------
	char *resRemappingSet(
		size_t &size, bool toMaster,
		uint16_t instanceId, uint32_t requestId, bool success,
		uint32_t listId, uint32_t chunkId,
		uint32_t *original, uint32_t *remapped, uint32_t remappedCount,
		uint8_t keySize, char *key
	);
	char *resRemapParity( size_t &size, uint16_t instanceId, uint32_t requestId );

	char *reqSet(
		size_t &size, uint16_t instanceId, uint32_t requestId,
		char *key, uint8_t keySize,
		char *value, uint32_t valueSize,
		char *buf = 0
	);
	char *reqRemappedUpdate(
		size_t &size, uint16_t instanceId, uint32_t requestId,
		char *key, uint8_t keySize,
		char *valueUpdate, uint32_t valueUpdateOffset, uint32_t valueUpdateSize,
		char *buf = 0, uint32_t timestamp = 0
	);
	char *reqRemappedDelete(
		size_t &size, uint16_t instanceId, uint32_t requestId,
		char *key, uint8_t keySize,
		char *buf = 0, uint32_t timestamp = 0
	);
	char *resRemappedUpdate(
		size_t &size, uint16_t instanceId, uint32_t requestId, bool success,
		char *key, uint8_t keySize,
		uint32_t valueUpdateOffset, uint32_t valueUpdateSize,
		char *buf = 0, uint32_t timestamp = 0
	);
	char *resRemappedDelete(
		size_t &size, uint16_t instanceId, uint32_t requestId, bool success,
		char *key, uint8_t keySize,
		char *buf = 0, uint32_t timestamp = 0
	);

	// ---------- degraded_protocol.cc ----------
	char *resReleaseDegradedLock( size_t &size, uint16_t instanceId, uint32_t requestId, uint32_t count );

	char *reqForwardKey(
		size_t &size, uint16_t instanceId, uint32_t requestId,
		uint8_t opcode, uint32_t listId, uint32_t stripeId, uint32_t chunkId,
		uint8_t keySize, char *key,
		uint32_t valueSize, char *value,
		uint32_t valueUpdateSize = 0, uint32_t valueUpdateOffset = 0, char *valueUpdate = 0
	);
	char *resForwardKey(
		size_t &size, uint16_t instanceId, uint32_t requestId, bool success,
		uint8_t opcode, uint32_t listId, uint32_t stripeId, uint32_t chunkId,
		uint8_t keySize, char *key,
		uint32_t valueSize,
		uint32_t valueUpdateSize = 0, uint32_t valueUpdateOffset = 0
	);

	char *reqGet(
		size_t &size, uint16_t instanceId, uint32_t requestId,
		uint32_t listId, uint32_t chunkId,
		uint8_t keySize, char *key
	);

	char *reqForwardChunk(
		size_t &size, uint16_t instanceId, uint32_t requestId,
		uint32_t listId, uint32_t stripeId, uint32_t chunkId,
		uint32_t chunkSize, uint32_t chunkOffset, char *chunkData
	);
	char *resForwardChunk(
		size_t &size, uint16_t instanceId, uint32_t requestId, bool success,
		uint32_t listId, uint32_t stripeId, uint32_t chunkId
	);

	// ---------- seal_protocol.cc ----------
	char *reqSealChunk( size_t &size, uint16_t instanceId, uint32_t requestId, Chunk *chunk, uint32_t startPos, char *buf = 0 );

	// ---------- recovery_protocol.cc ----------
	char *resServerReconstructedMsg( size_t &size, uint16_t instanceId, uint32_t requestId );
	char *resReconstruction( size_t &size, uint16_t instanceId, uint32_t requestId, uint32_t listId, uint32_t chunkId, uint32_t numStripes );
	char *resReconstructionUnsealed( size_t &size, uint16_t instanceId, uint32_t requestId, uint32_t listId, uint32_t chunkId, uint32_t numUnsealedKeys );
	char *resPromoteBackupServer( size_t &size, uint16_t instanceId, uint32_t requestId, uint32_t addr, uint16_t port, uint32_t numStripes, uint32_t numUnsealedKeys );
	char *reqBatchGetChunks(
		size_t &size, uint16_t instanceId, uint32_t requestId,
		std::vector<uint32_t> *requestIds,
		std::vector<Metadata> *metadata,
		uint32_t &chunksCount,
		bool &isCompleted
	);
	char *sendUnsealedKeys(
		size_t &size, uint16_t instanceId, uint32_t requestId,
		std::unordered_set<Key> &keys, std::unordered_set<Key>::iterator &it,
		std::unordered_map<Key, KeyValue> *values, LOCK_T *lock,
		uint32_t &keyValuesCount,
		bool &isCompleted
	);
	char *resUnsealedKeys(
		size_t &size, uint16_t instanceId, uint32_t requestId, bool success,
		struct BatchKeyValueHeader &header
	);

	// ---------- normal_server_protocol.cc ----------
	char *reqUpdate(
		size_t &size, uint16_t instanceId, uint32_t requestId,
		uint32_t listId, uint32_t stripeId, uint32_t chunkId,
		char *key, uint8_t keySize,
		char *valueUpdate, uint32_t valueUpdateOffset, uint32_t valueUpdateSize,
		uint32_t chunkUpdateOffset, char *buf = 0, uint32_t timestamp = 0
	);
	char *resUpdate(
		size_t &size, uint16_t instanceId, uint32_t requestId, bool success,
		uint32_t listId, uint32_t stripeId, uint32_t chunkId,
		char *key, uint8_t keySize, uint32_t valueUpdateOffset, uint32_t valueUpdateSize,
		uint32_t chunkUpdateOffset, char *buf = 0
	);

	char *reqDelete(
		size_t &size, uint16_t instanceId, uint32_t requestId,
		uint32_t listId, uint32_t stripeId, uint32_t chunkId,
		char *key, uint8_t keySize, char *buf = 0, uint32_t timestamp = 0
	);
	char *resDelete(
		size_t &size, uint16_t instanceId, uint32_t requestId, bool success,
		uint32_t listId, uint32_t stripeId, uint32_t chunkId,
		char *key, uint8_t keySize, char *buf = 0
	);

	char *reqGetChunk(
		size_t &size, uint16_t instanceId, uint32_t requestId,
		uint32_t listId, uint32_t stripeId, uint32_t chunkId,
		char *buf = 0
	);
	char *resGetChunk(
		size_t &size, uint16_t instanceId, uint32_t requestId, bool success,
		uint32_t listId, uint32_t stripeId, uint32_t chunkId,
		uint32_t chunkSize = 0, uint32_t chunkOffset = 0, char *chunkData = 0,
		uint8_t sealIndicatorCount = 0, bool *sealIndicator = 0
	);

	char *reqSetChunk(
		size_t &size, uint16_t instanceId, uint32_t requestId,
		uint32_t listId, uint32_t stripeId, uint32_t chunkId,
		uint32_t chunkSize, uint32_t chunkOffset, char *chunkData
	);
	char *reqSetChunk(
		size_t &size, uint16_t instanceId, uint32_t requestId,
		uint32_t listId, uint32_t stripeId, uint32_t chunkId,
		std::unordered_map<Key, KeyValue> *values,
		std::unordered_multimap<Metadata, Key> *metadataRev,
		std::unordered_set<Key> *deleted,
		LOCK_T *lock, bool &isCompleted
	);
	char *resSetChunk(
		size_t &size, uint16_t instanceId, uint32_t requestId, bool success,
		uint32_t listId, uint32_t stripeId, uint32_t chunkId
	);

	char *reqUpdateChunk(
		size_t &size, uint16_t instanceId, uint32_t requestId,
		uint32_t listId, uint32_t stripeId, uint32_t chunkId,
		uint32_t offset, uint32_t length, uint32_t updatingChunkId,
		char *delta, char *buf = 0, uint32_t timestamp = 0,
		bool checkGetChunk = false
	);
	char *resUpdateChunk(
		size_t &size, uint16_t instanceId, uint32_t requestId, bool success,
		uint32_t listId, uint32_t stripeId, uint32_t chunkId,
		uint32_t offset, uint32_t length, uint32_t updatingChunkId
	);

	char *reqDeleteChunk(
		size_t &size, uint16_t instanceId, uint32_t requestId,
		uint32_t listId, uint32_t stripeId, uint32_t chunkId,
		uint32_t offset, uint32_t length, uint32_t updatingChunkId,
		char *delta, char *buf = 0, uint32_t timestamp = 0,
		bool checkGetChunk = false
	);
	char *resDeleteChunk(
		size_t &size, uint16_t instanceId, uint32_t requestId, bool success,
		uint32_t listId, uint32_t stripeId, uint32_t chunkId,
		uint32_t offset, uint32_t length, uint32_t updatingChunkId
	);

	// ---------- ack_protocol.cc ----------
	char *ackMetadata( size_t &size, uint16_t instanceId, uint32_t requestId, uint32_t fromTimestamp, uint32_t toTimestamp );
	char *ackParityDeltaBackup( size_t &size, uint16_t instanceId, uint32_t requestId, std::vector<uint32_t> timestamps, uint16_t targetId );
	char *resRevertDelta( size_t &size, uint16_t instanceId, uint32_t requestId, bool success, std::vector<uint32_t> timestamps, std::vector<Key> requests, uint16_t targetId );
};
#endif

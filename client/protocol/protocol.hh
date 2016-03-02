#ifndef __CLIENT_PROTOCOL_PROTOCOL_HH__
#define __CLIENT_PROTOCOL_PROTOCOL_HH__

#include "../../common/config/server_addr.hh"
#include "../../common/ds/array_map.hh"
#include "../../common/ds/bitmask_array.hh"
#include "../../common/ds/latency.hh"
#include "../../common/ds/sockaddr_in.hh"
#include "../../common/protocol/protocol.hh"

class MasterProtocol : public Protocol {
public:
	MasterProtocol() : Protocol( ROLE_CLIENT ) {}

	// ---------- register_protocol.cc ----------
	char *reqRegisterCoordinator( size_t &size, uint32_t requestId, uint32_t addr, uint16_t port );
	char *reqRegisterSlave( size_t &size, uint16_t instanceId, uint32_t requestId, uint32_t addr, uint16_t port );
	char *resRegisterApplication( size_t &size, uint16_t instanceId, uint32_t requestId, bool success );

	// ---------- load_protocol.cc ----------
	char *reqPushLoadStats(
		size_t &size, uint16_t instanceId, uint32_t requestId,
		ArrayMap<struct sockaddr_in, Latency> *serverGetLatency,
		ArrayMap<struct sockaddr_in, Latency> *serverSetLatency
	);
	bool parseLoadingStats(
		const LoadStatsHeader& loadStatsHeader,
		ArrayMap<struct sockaddr_in, Latency> &serverGetLatency,
		ArrayMap<struct sockaddr_in, Latency> &serverSetLatency,
		std::set<struct sockaddr_in> &overloadedSlaveSet,
		char* buffer, uint32_t size
	);

	// ---------- normal_protocol.cc ----------
	char *reqSet(
		size_t &size, uint16_t instanceId, uint32_t requestId,
		char *key, uint8_t keySize,
		char *value, uint32_t valueSize,
		char *buf = 0
	);
	char *reqGet(
		size_t &size, uint16_t instanceId, uint32_t requestId,
		char *key, uint8_t keySize
	);
	char *reqUpdate(
		size_t &size, uint16_t instanceId, uint32_t requestId,
		char *key, uint8_t keySize,
		char *valueUpdate, uint32_t valueUpdateOffset, uint32_t valueUpdateSize,
		uint32_t timestamp = 0,
		bool checkGetChunk = false
	);
	char *reqDelete(
		size_t &size, uint16_t instanceId, uint32_t requestId,
		char *key, uint8_t keySize,
		uint32_t timestamp = 0,
		bool checkGetChunk = false
	);

	char *resSet(
		size_t &size, uint16_t instanceId, uint32_t requestId, bool success,
		uint8_t keySize, char *key
	);
	char *resGet(
		size_t &size, uint16_t instanceId, uint32_t requestId, bool success,
		uint8_t keySize, char *key,
		uint32_t valueSize = 0, char *value = 0
	);
	char *resUpdate(
		size_t &size, uint16_t instanceId, uint32_t requestId, bool success,
		uint8_t keySize, char *key,
		uint32_t valueUpdateOffset, uint32_t valueUpdateSize
	);
	char *resDelete(
		size_t &size, uint16_t instanceId, uint32_t requestId, bool success,
		uint8_t keySize, char *key
	);

	// ---------- remap_protocol.cc ----------
	char *reqRemappingSetLock(
		size_t &size, uint16_t instanceId, uint32_t requestId,
		uint32_t *original, uint32_t *remapped, uint32_t remappedCount,
		char *key, uint8_t keySize
	);
	char *reqRemappingSet(
		size_t &size, uint16_t instanceId, uint32_t requestId,
		uint32_t listId, uint32_t chunkId,
		uint32_t *original, uint32_t *remapped, uint32_t remappedCount,
		char *key, uint8_t keySize,
		char *value, uint32_t valueSize,
		char *buf = 0
	);

	// ---------- degraded_protocol.cc ----------
	char *reqDegradedLock(
		size_t &size, uint16_t instanceId, uint32_t requestId,
		uint32_t *original, uint32_t *reconstructed, uint32_t reconstructedCount,
		char *key, uint8_t keySize
	);
	char *reqDegradedGet(
		size_t &size, uint16_t instanceId, uint32_t requestId,
		bool isSealed, uint32_t stripeId,
		uint32_t *original, uint32_t *reconstructed, uint32_t reconstructedCount,
		uint32_t ongoingAtChunk, uint8_t numSurvivingChunkIds, uint32_t *survivingChunkIds,
		char *key, uint8_t keySize
	);
	char *reqDegradedUpdate(
		size_t &size, uint16_t instanceId, uint32_t requestId,
		bool isSealed, uint32_t stripeId,
		uint32_t *original, uint32_t *reconstructed, uint32_t reconstructedCount,
		uint32_t ongoingAtChunk, uint8_t numSurvivingChunkIds, uint32_t *survivingChunkIds,
		char *key, uint8_t keySize,
		char *valueUpdate, uint32_t valueUpdateOffset, uint32_t valueUpdateSize,
		uint32_t timestamp
	);
	char *reqDegradedDelete(
		size_t &size, uint16_t instanceId, uint32_t requestId,
		bool isSealed, uint32_t stripeId,
		uint32_t *original, uint32_t *reconstructed, uint32_t reconstructedCount,
		uint32_t ongoingAtChunk, uint8_t numSurvivingChunkIds, uint32_t *survivingChunkIds,
		char *key, uint8_t keySize,
		uint32_t timestamp
	);

	// ---------- fault_protocol.cc ----------
	char *syncMetadataBackup(
		size_t &size, uint16_t instanceId, uint32_t requestId,
		uint32_t addr, uint16_t port,
		LOCK_T *lock,
		std::unordered_multimap<uint32_t, Metadata> &sealed, uint32_t &sealedCount,
		std::unordered_map<Key, MetadataBackup> &ops, uint32_t &opsCount,
		bool &isCompleted
	);

	char *ackParityDeltaBackup(
		size_t &size, uint16_t instanceId, uint32_t requestId,
		std::vector<uint32_t> timestamps, uint16_t targetId
	);

	char *revertDelta(
		size_t &size, uint16_t instanceId, uint32_t requestId,
		std::vector<uint32_t> timestamps, std::vector<Key> requests,
		uint16_t targetId
	);

	// ---------- replay_protocol.cc ----------
	// same protocol as in application, but writes header into buffer.recv by default
	char *replaySet(
		size_t &size, uint16_t instanceId, uint32_t requestId,
		char *key, uint8_t keySize, char *value, uint32_t valueSize
	);

	char *replayGet(
		size_t &size, uint16_t instanceId, uint32_t requestId,
		char *key, uint8_t keySize
	);

	char *replayUpdate(
		size_t &size, uint16_t instanceId, uint32_t requestId,
		char *key, uint8_t keySize,
		char *valueUpdate, uint32_t valueUpdateOffset, uint32_t valueUpdateSize
	);

	char *replayDelete(
		size_t &size, uint16_t instanceId, uint32_t requestId,
		char *key, uint8_t keySize
	);
};

#endif

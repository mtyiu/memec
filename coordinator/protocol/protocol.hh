#ifndef __COORDINATOR_PROTOCOL_PROTOCOL_HH__
#define __COORDINATOR_PROTOCOL_PROTOCOL_HH__

#include <unordered_map>
#include "../socket/server_socket.hh"
#include "../../common/ds/latency.hh"
#include "../../common/ds/metadata.hh"
#include "../../common/lock/lock.hh"
#include "../../common/protocol/protocol.hh"
#include "../../common/config/global_config.hh"
#include "../../common/config/server_addr.hh"
#include "../../client/config/client_config.hh"
#include "../../server/config/server_config.hh"

class CoordinatorProtocol : public Protocol {
public:
	CoordinatorProtocol() : Protocol( ROLE_COORDINATOR ) {}

	// ---------- protocol.cc ----------
	char *reqSyncMeta( size_t &size, uint16_t instanceId, uint32_t requestId );
	char *reqSealChunks( size_t &size, uint16_t instanceId, uint32_t requestId );
	char *reqFlushChunks( size_t &size, uint16_t instanceId, uint32_t requestId );

	// ---------- register_protocol.cc ----------
	char *resRegisterClient( size_t &size, uint16_t instanceId, uint32_t requestId, bool success );
	char *resRegisterServer( size_t &size, uint16_t instanceId, uint32_t requestId, bool success );
	char *announceServerConnected( size_t &size, uint16_t instanceId, uint32_t requestId, ServerSocket *socket );

	// ---------- load_protocol.cc ----------
	char *reqPushLoadStats(
		size_t &size, uint16_t instanceId, uint32_t requestId,
		ArrayMap< struct sockaddr_in, Latency > *slaveGetLatency,
		ArrayMap< struct sockaddr_in, Latency > *slaveSetLatency,
		std::set< struct sockaddr_in > *overloadedServerSet
	);
	bool parseLoadingStats(
		const LoadStatsHeader& loadStatsHeader,
		ArrayMap< struct sockaddr_in, Latency >& slaveGetLatency,
		ArrayMap< struct sockaddr_in, Latency >& slaveSetLatency,
		char* buffer, uint32_t size
	);

	// ---------- degraded_protocol.cc ----------
	char *resDegradedLock(
		size_t &size, uint16_t instanceId, uint32_t requestId,
		bool isLocked, uint8_t keySize, char *key,
		bool isSealed, uint32_t stripeId, uint32_t dataChunkId, uint32_t dataChunkCount,
		uint32_t *original, uint32_t *reconstructed, uint32_t reconstructedCount,
		uint32_t ongoingAtChunk, uint8_t numSurvivingChunkIds, uint32_t *survivingChunkIds
	);
	char *resDegradedLock(
		size_t &size, uint16_t instanceId, uint32_t requestId,
		uint8_t keySize, char *key,
		uint32_t *original, uint32_t *remapped, uint32_t remappedCount
	);
	char *resDegradedLock(
		size_t &size, uint16_t instanceId, uint32_t requestId,
		bool exist,
		uint8_t keySize, char *key
	);
	char *reqReleaseDegradedLock(
		size_t &size, uint16_t instanceId, uint32_t requestId,
		std::vector<Metadata> &chunks,
		bool &isCompleted
	);

	// ---------- remap_protocol.cc ----------
	char *resRemappingSetLock(
		size_t &size, uint16_t instanceId, uint32_t requestId, bool success,
		uint32_t *original, uint32_t *remapped, uint32_t remappedCount,
		uint8_t keySize, char *key
	);
	char *reqSyncRemappedData( size_t &size, uint16_t instanceId, uint32_t requestId, struct sockaddr_in target, char* buffer = 0 );

	// ---------- recovery_protocol.cc ----------
	char *announceServerReconstructed(
		size_t &size, uint16_t instanceId, uint32_t requestId,
		ServerSocket *srcSocket, ServerSocket *dstSocket,
		bool toServer
	);
	char *promoteBackupServer(
		size_t &size, uint16_t instanceId, uint32_t requestId,
		ServerSocket *srcSocket,
		std::unordered_set<Metadata> &chunks,
		std::unordered_set<Metadata>::iterator &chunksIt,
		std::unordered_set<Key> &keys,
		std::unordered_set<Key>::iterator &keysIt,
		bool &isCompleted
	);
	char *reqReconstruction(
		size_t &size, uint16_t instanceId, uint32_t requestId,
		uint32_t listId, uint32_t chunkId,
		std::unordered_set<uint32_t> &stripeIds,
		std::unordered_set<uint32_t>::iterator &it,
		uint32_t numChunks,
		bool &isCompleted
	);
	char *reqReconstructionUnsealed(
		size_t &size, uint16_t instanceId, uint32_t requestId,
		std::unordered_set<Key> &keys, std::unordered_set<Key>::iterator &it, uint32_t &keysCount,
		bool &isCompleted
	);
	char *ackCompletedReconstruction( size_t &size, uint16_t instanceId, uint32_t requestId, bool success );

	// ---------- heartbeat_protocol.cc ----------
	char *resHeartbeat(
		size_t &size, uint16_t instanceId, uint32_t requestId,
		uint32_t timestamp,
		uint32_t sealed,
		uint32_t keys,
		bool isLast
	);
};

#endif

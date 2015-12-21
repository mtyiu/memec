#ifndef __COORDINATOR_PROTOCOL_PROTOCOL_HH__
#define __COORDINATOR_PROTOCOL_PROTOCOL_HH__

#include <unordered_map>
#include "../socket/slave_socket.hh"
#include "../../common/ds/latency.hh"
#include "../../common/ds/metadata.hh"
#include "../../common/lock/lock.hh"
#include "../../common/protocol/protocol.hh"
#include "../../common/config/global_config.hh"
#include "../../common/config/server_addr.hh"
#include "../../master/config/master_config.hh"
#include "../../slave/config/slave_config.hh"

class CoordinatorProtocol : public Protocol {
public:
	CoordinatorProtocol() : Protocol( ROLE_COORDINATOR ) {}

	// ---------- protocol.cc ----------
	char *reqSyncMeta( size_t &size, uint32_t id );
	char *reqSealChunks( size_t &size, uint32_t id );
	char *reqFlushChunks( size_t &size, uint32_t id );

	// ---------- register_protocol.cc ----------
	char *resRegisterMaster( size_t &size, uint32_t id, bool success );
	char *resRegisterSlave( size_t &size, uint32_t id, bool success );
	char *announceSlaveConnected( size_t &size, uint32_t id, SlaveSocket *socket );

	// ---------- load_protocol.cc ----------
	char *reqPushLoadStats(
		size_t &size, uint32_t id,
		ArrayMap< struct sockaddr_in, Latency > *slaveGetLatency,
		ArrayMap< struct sockaddr_in, Latency > *slaveSetLatency,
		std::set< struct sockaddr_in > *overloadedSlaveSet
	);
	bool parseLoadingStats(
		const LoadStatsHeader& loadStatsHeader,
		ArrayMap< struct sockaddr_in, Latency >& slaveGetLatency,
		ArrayMap< struct sockaddr_in, Latency >& slaveSetLatency,
		char* buffer, uint32_t size
	);

	// ---------- degraded_protocol.cc ----------
	char *resDegradedLock(
		size_t &size, uint32_t id,
		bool isLocked, bool isSealed,
		uint8_t keySize, char *key,
		uint32_t listId, uint32_t stripeId,
		uint32_t srcDataChunkId, uint32_t dstDataChunkId,
		uint32_t srcParityChunkId, uint32_t dstParityChunkId
	);
	char *resDegradedLock(
		size_t &size, uint32_t id,
		uint8_t keySize, char *key,
		uint32_t listId,
		uint32_t srcDataChunkId, uint32_t dstDataChunkId,
		uint32_t srcParityChunkId, uint32_t dstParityChunkId
	);
	char *resDegradedLock(
		size_t &size, uint32_t id,
		bool exist,
		uint8_t keySize, char *key,
		uint32_t listId, uint32_t srcDataChunkId, uint32_t srcParityChunkId
	);
	char *reqReleaseDegradedLock(
		size_t &size, uint32_t id,
		std::vector<Metadata> &chunks,
		bool &isCompleted
	);

	// ---------- remap_protocol.cc ----------
	// RemapList
	bool parseRemappingLockHeader(
		struct RemappingLockHeader &header,
		char *buf, size_t size,
		std::vector<uint32_t> *remapList = 0,
		size_t offset = 0
	);
	char *resRemappingSetLock(
		size_t &size, uint32_t id, bool success,
		uint32_t listId, uint32_t chunkId,
		bool isRemapped, uint8_t keySize, char *key,
		uint32_t sockfd = UINT_MAX
	);
	// Forward the whole remapping record message passed in (with the protocol header excluded) pfrom slave to masters
	char *forwardRemappingRecords( size_t &size, uint32_t id, char *message );
	// Remapping
	char *reqSyncRemappingRecord(
		size_t &size, uint32_t id,
		std::unordered_map<Key, RemappingRecord> &remappingRecords,
		LOCK_T* lock,
		bool &isLast,
		char *buffer = 0
	);
	char *reqSyncRemappedData( size_t &size, uint32_t id, struct sockaddr_in target, char* buffer = 0 );

	// ---------- recovery_protocol.cc ----------
	char *announceSlaveReconstructed( size_t &size, uint32_t id, SlaveSocket *srcSocket, SlaveSocket *dstSocket );
	char *promoteBackupSlave(
		size_t &size, uint32_t id,
		SlaveSocket *srcSocket,
		std::unordered_set<Metadata> &chunks,
		std::unordered_set<Metadata>::iterator &it,
		bool &isCompleted
	);
	char *reqReconstruction(
		size_t &size, uint32_t id,
		uint32_t listId, uint32_t chunkId,
		std::unordered_set<uint32_t> &stripeIds,
		std::unordered_set<uint32_t>::iterator &it,
		uint32_t numChunks,
		bool &isCompleted
	);

};

#endif

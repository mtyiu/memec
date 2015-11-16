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

	/* Master */
	// Register
	char *resRegisterMaster( size_t &size, uint32_t id, bool success );
	// char *resRegisterMaster( size_t &size, GlobalConfig &globalConfig, MasterConfig *masterConfig = 0 );
	// Load statistics
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
	// Forward the whole remapping record message passed in (with the protocol header excluded) pfrom slave to masters
	char *forwardRemappingRecords( size_t &size, uint32_t id, char *message );
	// Degraded operation
	char *resDegradedLock(
		size_t &size, uint32_t id,
		uint8_t keySize, char *key, bool isLocked, bool isSealed,
		uint32_t srcListId, uint32_t srcStripeId, uint32_t srcChunkId,
		uint32_t dstListId, uint32_t dstChunkId
	);
	char *resDegradedLock(
		size_t &size, uint32_t id,
		uint8_t keySize, char *key,
		uint32_t listId, uint32_t chunkId
	);
	char *resDegradedLock(
		size_t &size, uint32_t id,
		uint8_t keySize, char *key
	);

	/* Slave */
	// Register
	char *resRegisterSlave( size_t &size, uint32_t id, bool success );
	// char *resRegisterSlave( size_t &size, GlobalConfig &globalConfig, SlaveConfig *slaveConfig = 0 );
	char *announceSlaveConnected( size_t &size, uint32_t id, SlaveSocket *socket );
	char *announceSlaveReconstructed( size_t &size, uint32_t id, SlaveSocket *srcSocket, SlaveSocket *dstSocket );
	char *reqSealChunks( size_t &size, uint32_t id );
	char *reqFlushChunks( size_t &size, uint32_t id );
	char *reqSyncMeta( size_t &size, uint32_t id );
	char *reqReleaseDegradedLock(
		size_t &size, uint32_t id,
		std::vector<Metadata> &chunks,
		bool &isCompleted
	);
	char *reqSyncRemappingRecord( size_t &size, uint32_t id, std::unordered_map<Key, RemappingRecord> &remappingRecords, LOCK_T* lock, bool &isLast, char *buffer = 0 );
	char *resRemappingSetLock( size_t &size, uint32_t id, bool success, uint32_t listId, uint32_t chunkId, bool isRemapped, uint8_t keySize, char *key );
	// Recovery
	char *reqRecovery(
		size_t &size, uint32_t id,
		uint32_t listId, uint32_t chunkId,
		std::unordered_set<uint32_t> &stripeIds,
		std::unordered_set<uint32_t>::iterator &it,
		uint32_t numChunks,
		bool &isCompleted
	);
};

#endif

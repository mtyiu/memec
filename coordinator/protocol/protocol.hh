#ifndef __COORDINATOR_PROTOCOL_PROTOCOL_HH__
#define __COORDINATOR_PROTOCOL_PROTOCOL_HH__

#include "../socket/slave_socket.hh"
#include "../../common/ds/latency.hh"
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

	/* Slave */
	// Register
	char *resRegisterSlave( size_t &size, uint32_t id, bool success );
	// char *resRegisterSlave( size_t &size, GlobalConfig &globalConfig, SlaveConfig *slaveConfig = 0 );
	char *announceSlaveConnected( size_t &size, uint32_t id, SlaveSocket *socket );
	char *reqSealChunks( size_t &size, uint32_t id );
	char *reqFlushChunks( size_t &size, uint32_t id );
	char *reqSyncMeta( size_t &size, uint32_t id );
	char *resRemappingSetLock( size_t &size, uint32_t id, bool success, uint32_t listId, uint32_t chunkId, bool isRemapped, uint8_t keySize, char *key );
};

#endif

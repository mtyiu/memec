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
	bool parseLoadingStats(
		const LoadStatsHeader& loadStatsHeader,
		ArrayMap< ServerAddr, Latency >& slaveGetLatency,
		ArrayMap< ServerAddr, Latency >& slaveSetLatency,
		char* buffer, uint32_t size
	);

	/* Slave */
	// Register
	char *resRegisterSlave( size_t &size, uint32_t id, bool success );
	// char *resRegisterSlave( size_t &size, GlobalConfig &globalConfig, SlaveConfig *slaveConfig = 0 );
	char *announceSlaveConnected( size_t &size, uint32_t id, SlaveSocket *socket );
};

#endif

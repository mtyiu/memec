#ifndef __COORDINATOR_PROTOCOL_PROTOCOL_HH__
#define __COORDINATOR_PROTOCOL_PROTOCOL_HH__

#include "../../common/protocol/protocol.hh"
#include "../../common/config/global_config.hh"
#include "../../master/config/master_config.hh"
#include "../../slave/config/slave_config.hh"

class CoordinatorProtocol : public Protocol {
public:
	CoordinatorProtocol() : Protocol( ROLE_COORDINATOR ) {}
	
	/* Master */
	// Register
	char *resRegisterMaster( size_t &size, bool success );
	char *resRegisterMaster( size_t &size, GlobalConfig &globalConfig, MasterConfig *masterConfig = 0 );

	/* Slave */
	// Register
	char *resRegisterSlave( size_t &size, bool success );
	char *resRegisterSlave( size_t &size, GlobalConfig &globalConfig, SlaveConfig *slaveConfig = 0 );
};

#endif

#ifndef __COORDINATOR_PROTOCOL_PROTOCOL_HH__
#define __COORDINATOR_PROTOCOL_PROTOCOL_HH__

#include "../../common/protocol/protocol.hh"
#include "../../common/config/global_config.hh"
#include "../../master/config/master_config.hh"
#include "../../slave/config/slave_config.hh"

class CoordinatorProtocol : public Protocol {
public:
	char *resMasterRegister( size_t &size );
	char *resMasterRegister( size_t &size, GlobalConfig &globalConfig, MasterConfig *masterConfig = 0 );
	char *resSlaveRegister( size_t &size );
	char *resSlaveRegister( size_t &size, GlobalConfig &globalConfig, SlaveConfig *slaveConfig = 0 );
};

#endif

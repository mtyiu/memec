#ifndef __SLAVE_PROTOCOL_PROTOCOL_HH__
#define __SLAVE_PROTOCOL_PROTOCOL_HH__

#include "../../common/protocol/protocol.hh"

class SlaveProtocol : public Protocol {
public:
	SlaveProtocol() : Protocol( ROLE_SLAVE ) {}
	char *reqRegisterCoordinator( size_t &size );
	char *reqRegisterSlavePeer( size_t &size );
	char *resRegisterMaster( size_t &size, bool success );
	char *resRegisterSlavePeer( size_t &size, bool success );
};
#endif

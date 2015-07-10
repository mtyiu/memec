#ifndef __MASTER_PROTOCOL_PROTOCOL_HH__
#define __MASTER_PROTOCOL_PROTOCOL_HH__

#include "../../common/protocol/protocol.hh"

class MasterProtocol : public Protocol {
public:
	MasterProtocol() : Protocol( ROLE_MASTER ) {}
	char *reqRegisterCoordinator( size_t &size );
	// char *resRegisterCoordinator( size_t &size );
};

#endif

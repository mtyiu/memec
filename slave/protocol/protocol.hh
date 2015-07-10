#ifndef __SLAVE_PROTOCOL_PROTOCOL_HH__
#define __SLAVE_PROTOCOL_PROTOCOL_HH__

#include "../../common/protocol/protocol.hh"

class SlaveProtocol : public Protocol {
public:
	SlaveProtocol() : Protocol( ROLE_SLAVE ) {}
	char *reqRegisterCoordinator( size_t &size );
	// char *resRegisterCoordinator( size_t &size );
};
#endif

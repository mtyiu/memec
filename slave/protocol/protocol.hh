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
	char *resSet( size_t &size, bool success, uint8_t keySize, char *key );
	char *resGet( size_t &size, bool success, uint8_t keySize, char *key, uint32_t valueSize = 0, char *value = 0 );
};
#endif

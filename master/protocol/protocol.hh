#ifndef __MASTER_PROTOCOL_PROTOCOL_HH__
#define __MASTER_PROTOCOL_PROTOCOL_HH__

#include "../../common/protocol/protocol.hh"

class MasterProtocol : public Protocol {
public:
	MasterProtocol() : Protocol( ROLE_MASTER ) {}
	char *reqRegisterCoordinator( size_t &size );
	char *reqRegisterSlave( size_t &size );
	char *resRegisterApplication( size_t &size, bool success );
	char *resSet( size_t &size, bool success, uint8_t keySize, char *key );
	char *resGet( size_t &size, bool success, uint8_t keySize, char *key, uint32_t valueSize = 0, char *value = 0 );
};

#endif

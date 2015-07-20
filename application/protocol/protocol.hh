#ifndef __APPLICATION_PROTOCOL_PROTOCOL_HH__
#define __APPLICATION_PROTOCOL_PROTOCOL_HH__

#include "../../common/protocol/protocol.hh"

class ApplicationProtocol : public Protocol {
public:
	ApplicationProtocol() : Protocol( ROLE_APPLICATION ) {}
	char *reqRegisterMaster( size_t &size );
};

#endif

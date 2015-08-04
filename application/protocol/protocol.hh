#ifndef __APPLICATION_PROTOCOL_PROTOCOL_HH__
#define __APPLICATION_PROTOCOL_PROTOCOL_HH__

#include "../../common/protocol/protocol.hh"

class ApplicationProtocol : public Protocol {
public:
	ApplicationProtocol() : Protocol( ROLE_APPLICATION ) {}
	char *reqRegisterMaster( size_t &size );
	char *reqSet( size_t &size, char *key, uint8_t keySize, char *value, uint32_t valueSize );
	char *reqGet( size_t &size, char *key, uint8_t keySize );
	char *reqUpdate( size_t &size, char *key, uint8_t keySize, char *valueUpdate, uint32_t valueUpdateOffset, uint32_t valueUpdateSize );
	char *reqDelete( size_t &size, char *key, uint8_t keySize );
};

#endif

#ifndef __APPLICATION_PROTOCOL_PROTOCOL_HH__
#define __APPLICATION_PROTOCOL_PROTOCOL_HH__

#include "../../common/protocol/protocol.hh"

class ApplicationProtocol : public Protocol {
public:
	ApplicationProtocol() : Protocol( ROLE_APPLICATION ) {}

	/* Client */
	// Register
	char *reqRegisterClient( size_t &size, uint32_t requestId );
	// SET
	char *reqSet( size_t &size, uint16_t instanceId, uint32_t requestId, char *key, uint8_t keySize, char *value, uint32_t valueSize );
	// GET
	char *reqGet( size_t &size, uint16_t instanceId, uint32_t requestId, char *key, uint8_t keySize );
	// UPDATE
	char *reqUpdate( size_t &size, uint16_t instanceId, uint32_t requestId, char *key, uint8_t keySize, char *valueUpdate, uint32_t valueUpdateOffset, uint32_t valueUpdateSize );
	// DELETE
	char *reqDelete( size_t &size, uint16_t instanceId, uint32_t requestId, char *key, uint8_t keySize );
};

#endif

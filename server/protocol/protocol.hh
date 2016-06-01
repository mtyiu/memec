#ifndef __SERVER_PROTOCOL_PROTOCOL_HH__
#define __SERVER_PROTOCOL_PROTOCOL_HH__

#include "../../common/ds/chunk.hh"
#include "../../common/protocol/protocol.hh"

class ServerProtocol : public Protocol {
public:
	ServerProtocol() : Protocol( ROLE_SERVER ) {}

	char *reqSealChunk( size_t &size, uint16_t instanceId, uint32_t requestId, Chunk *chunk, uint32_t startPos, char *buf = 0 );

	char *reqRemappedDelete(
		size_t &size, uint16_t instanceId, uint32_t requestId,
		char *key, uint8_t keySize,
		char *buf = 0, uint32_t timestamp = 0
	);
};
#endif

#ifndef __SLAVE_PROTOCOL_PROTOCOL_HH__
#define __SLAVE_PROTOCOL_PROTOCOL_HH__

#include "../../common/protocol/protocol.hh"

class SlaveProtocol : public Protocol {
public:
	SlaveProtocol() : Protocol( ROLE_SLAVE ) {}
	
	/* Coordinator */
	// Register
	char *reqRegisterCoordinator( size_t &size );
	// Heartbeat
	char *sendHeartbeat( size_t &size, struct HeartbeatHeader &header, std::map<Key, OpMetadata> &opMetadataMap, size_t &count );

	/* Master */
	// Register
	char *resRegisterMaster( size_t &size, bool success );
	// SET
	char *resSet( size_t &size, bool success, uint8_t keySize, char *key );
	// GET
	char *resGet( size_t &size, bool success, uint8_t keySize, char *key, uint32_t valueSize = 0, char *value = 0 );
	// UPDATE
	char *resUpdate( size_t &size, uint8_t keySize, char *key, uint32_t listId, uint32_t stripeId, uint32_t chunkId, uint32_t offset, uint32_t length, char *delta );
	char *resUpdate( size_t &size, uint8_t keySize, char *key );
	// UPDATE_CHUNK
	char *resUpdateChunk( size_t &size, bool success, uint32_t listId, uint32_t stripeId, uint32_t chunkId, uint32_t offset, uint32_t length );
	// DELETE
	char *resDelete( size_t &size, uint8_t keySize, char *key, uint32_t listId, uint32_t stripeId, uint32_t chunkId, uint32_t offset, uint32_t length, char *delta );
	char *resDelete( size_t &size, uint8_t keySize, char *key );
	// DELETE_CHUNK
	char *resDeleteChunk( size_t &size, bool success, uint32_t listId, uint32_t stripeId, uint32_t chunkId, uint32_t offset, uint32_t length );

	/* Slave */
	// Register
	char *reqRegisterSlavePeer( size_t &size );
	char *resRegisterSlavePeer( size_t &size, bool success );
};
#endif

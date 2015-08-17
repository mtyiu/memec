#ifndef __SLAVE_PROTOCOL_PROTOCOL_HH__
#define __SLAVE_PROTOCOL_PROTOCOL_HH__

#include "../../common/protocol/protocol.hh"
#include "../../common/config/server_addr.hh"

class SlaveProtocol : public Protocol {
public:
	volatile bool *status; // Indicate which slave in the stripe is accessing the internal buffer

	SlaveProtocol() : Protocol( ROLE_SLAVE ) {}
	bool init( size_t size, uint32_t dataChunkCount );
	void free();

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
	char *resUpdate( size_t &size, bool success, uint8_t keySize, char *key, uint32_t valueUpdateOffset, uint32_t valueUpdateSize );
	// DELETE
	char *resDelete( size_t &size, bool success, uint8_t keySize, char *key );

	/* Slave */
	// Register
	char *reqRegisterSlavePeer( size_t &size, ServerAddr *addr );
	char *resRegisterSlavePeer( size_t &size, bool success );
	// UPDATE_CHUNK
	char *reqUpdateChunk( size_t &size, uint32_t listId, uint32_t stripeId, uint32_t chunkId, uint32_t offset, uint32_t length, uint32_t updatingChunkId, char *delta );
	char *resUpdateChunk( size_t &size, bool success, uint32_t listId, uint32_t stripeId, uint32_t chunkId, uint32_t offset, uint32_t length, uint32_t updatingChunkId );
	// DELETE_CHUNK
	char *reqDeleteChunk( size_t &size, uint32_t listId, uint32_t stripeId, uint32_t chunkId, uint32_t offset, uint32_t length, uint32_t updatingChunkId, char *delta );
	char *resDeleteChunk( size_t &size, bool success, uint32_t listId, uint32_t stripeId, uint32_t chunkId, uint32_t offset, uint32_t length, uint32_t updatingChunkId );
	// GET_CHUNK
	char *reqGetChunk( size_t &size, uint32_t listId, uint32_t stripeId, uint32_t chunkId );
	char *resGetChunk( size_t &size, bool success, uint32_t listId, uint32_t stripeId, uint32_t chunkId, uint32_t chunkSize = 0, char *chunkData = 0 );
	// SET_CHUNK
	char *reqSetChunk( size_t &size, uint32_t listId, uint32_t stripeId, uint32_t chunkId, uint32_t chunkSize, char *chunkData );
	char *resSetChunk( size_t &size, bool success, uint32_t listId, uint32_t stripeId, uint32_t chunkId );
};
#endif

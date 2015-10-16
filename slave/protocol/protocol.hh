#ifndef __SLAVE_PROTOCOL_PROTOCOL_HH__
#define __SLAVE_PROTOCOL_PROTOCOL_HH__

#include "../../common/protocol/protocol.hh"
#include "../../common/config/server_addr.hh"
#include "../../common/ds/chunk.hh"

class SlaveProtocol : public Protocol {
public:
	volatile bool *status; // Indicate which slave in the stripe is accessing the internal buffer

	SlaveProtocol() : Protocol( ROLE_SLAVE ) {}
	bool init( size_t size, uint32_t dataChunkCount );
	void free();

	/* Coordinator */
	// Register
	char *reqRegisterCoordinator( size_t &size, uint32_t id, uint32_t addr, uint16_t port );
	// Heartbeat
	char *sendHeartbeat( size_t &size, uint32_t id, std::map<Key, OpMetadata> &opMetadataMap, pthread_mutex_t *lock, size_t &count );
	// Remapping Records
	char *sendRemappingRecords( size_t &size, uint32_t id, std::map<Key, RemappingRecord> &remapRecord, pthread_mutex_t *lock, size_t &remapCount );

	/* Master */
	// Register
	char *resRegisterMaster( size_t &size, uint32_t id, bool success );
	// SET
	char *resSet( size_t &size, uint32_t id, bool success, uint8_t keySize, char *key );
	// REMAPPING_SET_LOCK
	char *resRemappingSetLock( size_t &size, uint32_t id, bool success, uint32_t listId, uint32_t chunkId, uint8_t keySize, char *key );
	// REMAPPING_SET
	char *resRemappingSet( size_t &size, bool toMaster, uint32_t id, bool success, uint32_t listId, uint32_t chunkId, uint8_t keySize, char *key );
	// GET
	char *resGet( size_t &size, uint32_t id, bool success, uint8_t keySize, char *key, uint32_t valueSize = 0, char *value = 0 );
	// UPDATE
	char *resUpdate( size_t &size, uint32_t id, bool success, uint8_t keySize, char *key, uint32_t valueUpdateOffset, uint32_t valueUpdateSize );
	// DELETE
	char *resDelete( size_t &size, uint32_t id, bool success, uint8_t keySize, char *key, bool toMaster = true );
	// Redirect
	char *resRedirect( size_t &size, uint32_t id, uint8_t opcode, uint8_t keySize, char *key, uint32_t remappedListId, uint32_t remappedChunkId );

	/* Slave */
	// Register
	char *reqRegisterSlavePeer( size_t &size, uint32_t id, ServerAddr *addr );
	char *resRegisterSlavePeer( size_t &size, uint32_t id, bool success );
	// REMAPPING_SET
	char *reqRemappingSet( size_t &size, uint32_t id, uint32_t listId, uint32_t chunkId, bool needsForwarding, char *key, uint8_t keySize, char *value, uint32_t valueSize, char *buf = 0 );
	// SEAL_CHUNK
	char *reqSealChunk( size_t &size, uint32_t id, Chunk *chunk, uint32_t startPos, char *buf = 0 );
	// UPDATE
	char *reqUpdate( size_t &size, uint32_t id, uint32_t listId, uint32_t stripeId, uint32_t chunkId, char *key, uint8_t keySize, char *valueUpdate, uint32_t valueUpdateOffset, uint32_t valueUpdateSize, uint32_t chunkUpdateOffset, char *buf = 0 );
	char *resUpdate( size_t &size, uint32_t id, bool success, uint32_t listId, uint32_t stripeId, uint32_t chunkId, char *key, uint8_t keySize, uint32_t valueUpdateOffset, uint32_t valueUpdateSize, uint32_t chunkUpdateOffset, char *buf = 0 );
	// DELETE
	char *reqDelete( size_t &size, uint32_t id, uint32_t listId, uint32_t stripeId, uint32_t chunkId, char *key, uint8_t keySize, char *buf = 0 );
	char *resDelete( size_t &size, uint32_t id, bool success, uint32_t listId, uint32_t stripeId, uint32_t chunkId, char *key, uint8_t keySize, char *buf = 0 );
	// UPDATE_CHUNK
	char *reqUpdateChunk( size_t &size, uint32_t id, uint32_t listId, uint32_t stripeId, uint32_t chunkId, uint32_t offset, uint32_t length, uint32_t updatingChunkId, char *delta, char *buf = 0 );
	char *resUpdateChunk( size_t &size, uint32_t id, bool success, uint32_t listId, uint32_t stripeId, uint32_t chunkId, uint32_t offset, uint32_t length, uint32_t updatingChunkId );
	// DELETE_CHUNK
	char *reqDeleteChunk( size_t &size, uint32_t id, uint32_t listId, uint32_t stripeId, uint32_t chunkId, uint32_t offset, uint32_t length, uint32_t updatingChunkId, char *delta, char *buf = 0 );
	char *resDeleteChunk( size_t &size, uint32_t id, bool success, uint32_t listId, uint32_t stripeId, uint32_t chunkId, uint32_t offset, uint32_t length, uint32_t updatingChunkId );
	// GET_CHUNK
	char *reqGetChunk( size_t &size, uint32_t id, uint32_t listId, uint32_t stripeId, uint32_t chunkId );
	char *resGetChunk( size_t &size, uint32_t id, bool success, uint32_t listId, uint32_t stripeId, uint32_t chunkId, uint32_t chunkSize = 0, char *chunkData = 0 );
	// SET_CHUNK
	char *reqSetChunk( size_t &size, uint32_t id, uint32_t listId, uint32_t stripeId, uint32_t chunkId, uint32_t chunkSize, char *chunkData );
	char *resSetChunk( size_t &size, uint32_t id, bool success, uint32_t listId, uint32_t stripeId, uint32_t chunkId );
};
#endif

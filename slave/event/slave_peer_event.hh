#ifndef __SLAVE_EVENT_SLAVE_PEER_EVENT_HH__
#define __SLAVE_EVENT_SLAVE_PEER_EVENT_HH__

#include "../protocol/protocol.hh"
#include "../socket/slave_peer_socket.hh"
#include "../../common/ds/metadata.hh"
#include "../../common/ds/chunk.hh"
#include "../../common/ds/packet_pool.hh"
#include "../../common/event/event.hh"

enum SlavePeerEventType {
	SLAVE_PEER_EVENT_TYPE_UNDEFINED,
	// Register
	SLAVE_PEER_EVENT_TYPE_REGISTER_REQUEST,
	SLAVE_PEER_EVENT_TYPE_REGISTER_RESPONSE_SUCCESS,
	SLAVE_PEER_EVENT_TYPE_REGISTER_RESPONSE_FAILURE,
	// REMAPPING_SET
	SLAVE_PEER_EVENT_TYPE_REMAPPING_SET_RESPONSE_SUCCESS,
	SLAVE_PEER_EVENT_TYPE_REMAPPING_SET_RESPONSE_FAILURE,
	// DELETE
	SLAVE_PEER_EVENT_TYPE_DELETE_RESPONSE_SUCCESS,
	SLAVE_PEER_EVENT_TYPE_DELETE_RESPONSE_FAILURE,
	// UPDATE
	SLAVE_PEER_EVENT_TYPE_UPDATE_RESPONSE_SUCCESS,
	SLAVE_PEER_EVENT_TYPE_UPDATE_RESPONSE_FAILURE,
	// UPDATE_CHUNK
	SLAVE_PEER_EVENT_TYPE_UPDATE_CHUNK_RESPONSE_SUCCESS,
	SLAVE_PEER_EVENT_TYPE_UPDATE_CHUNK_RESPONSE_FAILURE,
	// DELETE_CHUNK
	SLAVE_PEER_EVENT_TYPE_DELETE_CHUNK_RESPONSE_SUCCESS,
	SLAVE_PEER_EVENT_TYPE_DELETE_CHUNK_RESPONSE_FAILURE,
	// GET_CHUNK
	SLAVE_PEER_EVENT_TYPE_GET_CHUNK_REQUEST,
	SLAVE_PEER_EVENT_TYPE_GET_CHUNK_RESPONSE_SUCCESS,
	SLAVE_PEER_EVENT_TYPE_GET_CHUNK_RESPONSE_FAILURE,
	// SET_CHUNK
	SLAVE_PEER_EVENT_TYPE_SET_CHUNK_REQUEST,
	SLAVE_PEER_EVENT_TYPE_SET_CHUNK_RESPONSE_SUCCESS,
	SLAVE_PEER_EVENT_TYPE_SET_CHUNK_RESPONSE_FAILURE,
	// SEAL_CHUNK
	SLAVE_PEER_EVENT_TYPE_SEAL_CHUNK_REQUEST,
	SLAVE_PEER_EVENT_TYPE_SEAL_CHUNK_RESPONSE_SUCCESS,
	SLAVE_PEER_EVENT_TYPE_SEAL_CHUNK_RESPONSE_FAILURE,
	// Send
	SLAVE_PEER_EVENT_TYPE_SEND,
	// Pending
	SLAVE_PEER_EVENT_TYPE_PENDING
};

class SlavePeerEvent : public Event {
public:
	SlavePeerEventType type;
	uint32_t id;
	SlavePeerSocket *socket;
	struct {
		Key key;
		struct {
			Metadata metadata;
			uint32_t offset;
			uint32_t length;
			uint32_t updatingChunkId;
		} chunkUpdate;
		struct {
			uint32_t listId, stripeId, chunkId, offset, length;
			Key key;
		} update;
		struct {
			uint32_t listId, stripeId, chunkId;
			Key key;
		} del;
		struct {
			Metadata metadata;
			Chunk *chunk;
		} chunk;
		struct {
			Packet *packet;
		} send;
		struct {
			Key key;
			uint32_t listId, chunkId;
		} remap;
	} message;

	// Register
	void reqRegister( SlavePeerSocket *socket );
	void resRegister( SlavePeerSocket *socket, uint32_t id, bool success = true );
	// REMAPPING_SET
	void resRemappingSet( SlavePeerSocket *socket, uint32_t id, Key &key, uint32_t listId, uint32_t chunkId, bool success );
	// UPDATE
	void resUpdate( SlavePeerSocket *socket, uint32_t id, uint32_t listId, uint32_t stripeId, uint32_t chunkId, Key &key, uint32_t offset, uint32_t length, bool success );
	// DELETE
	void resDelete( SlavePeerSocket *socket, uint32_t id, uint32_t listId, uint32_t stripeId, uint32_t chunkId, Key &key, bool success );
	// UPDATE_CHUNK
	void resUpdateChunk( SlavePeerSocket *socket, uint32_t id, Metadata &metadata, uint32_t offset, uint32_t length, uint32_t updatingChunkId, bool success );
	// DELETE_CHUNK
	void resDeleteChunk( SlavePeerSocket *socket, uint32_t id, Metadata &metadata, uint32_t offset, uint32_t length, uint32_t updatingChunkId, bool success );
	// GET_CHUNK
	void reqGetChunk( SlavePeerSocket *socket, Metadata &metadata );
	void resGetChunk( SlavePeerSocket *socket, uint32_t id, Metadata &metadata, bool success, Chunk *chunk = 0 );
	// SET_CHUNK
	void reqSetChunk( SlavePeerSocket *socket, Metadata &metadata, Chunk *chunk );
	void resSetChunk( SlavePeerSocket *socket, uint32_t id, Metadata &metadata, bool success );
	// SEAL_CHUNK
	void reqSealChunk( Chunk *chunk );
	void resSealChunk( SlavePeerSocket *socket, uint32_t id, Metadata &metadata, bool success );
	// Send
	void send( SlavePeerSocket *socket, Packet *packet );
	// Pending
	void pending( SlavePeerSocket *socket );
};

#endif

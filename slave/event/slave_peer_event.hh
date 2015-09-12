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
	// Send
	SLAVE_PEER_EVENT_TYPE_SEND,
	// Pending
	SLAVE_PEER_EVENT_TYPE_PENDING
};

class SlavePeerEvent : public Event {
public:
	SlavePeerEventType type;
	SlavePeerSocket *socket;
	struct {
		struct {
			Metadata metadata;
			uint32_t offset;
			uint32_t length;
			uint32_t updatingChunkId;
		} chunkUpdate;
		struct {
			Metadata metadata;
			volatile bool *status;
			Chunk *chunk;
		} chunk;
		struct {
			Packet *packet;
		} send;
	} message;

	// Register
	void reqRegister( SlavePeerSocket *socket );
	void resRegister( SlavePeerSocket *socket, bool success = true );
	// UPDATE_CHUNK
	void resUpdateChunk( SlavePeerSocket *socket, Metadata &metadata, uint32_t offset, uint32_t length, uint32_t updatingChunkId, bool success );
	// DELETE_CHUNK
	void resDeleteChunk( SlavePeerSocket *socket, Metadata &metadata, uint32_t offset, uint32_t length, uint32_t updatingChunkId, bool success );
	// GET_CHUNK
	void reqGetChunk( SlavePeerSocket *socket, Metadata &metadata, volatile bool *status = 0 );
	void resGetChunk( SlavePeerSocket *socket, Metadata &metadata, bool success, Chunk *chunk = 0 );
	// SET_CHUNK
	void reqSetChunk( SlavePeerSocket *socket, Metadata &metadata, Chunk *chunk, volatile bool *status = 0 );
	void resSetChunk( SlavePeerSocket *socket, Metadata &metadata, bool success );
	// Send
	void send( SlavePeerSocket *socket, Packet *packet );
	// Pending
	void pending( SlavePeerSocket *socket );
};

#endif

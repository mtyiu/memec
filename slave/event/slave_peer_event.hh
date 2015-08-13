#ifndef __SLAVE_EVENT_SLAVE_PEER_EVENT_HH__
#define __SLAVE_EVENT_SLAVE_PEER_EVENT_HH__

#include "../protocol/protocol.hh"
#include "../socket/slave_peer_socket.hh"
#include "../../common/ds/metadata.hh"
#include "../../common/event/event.hh"

enum SlavePeerEventType {
	SLAVE_PEER_EVENT_TYPE_UNDEFINED,
	// Register
	SLAVE_PEER_EVENT_TYPE_REGISTER_REQUEST,
	SLAVE_PEER_EVENT_TYPE_REGISTER_RESPONSE_SUCCESS,
	SLAVE_PEER_EVENT_TYPE_REGISTER_RESPONSE_FAILURE,
	// UPDATE_CHUNK
	SLAVE_PEER_EVENT_TYPE_UPDATE_CHUNK_REQUEST,
	SLAVE_PEER_EVENT_TYPE_UPDATE_CHUNK_RESPONSE_SUCCESS,
	SLAVE_PEER_EVENT_TYPE_UPDATE_CHUNK_RESPONSE_FAILURE,
	// DELETE_CHUNK
	SLAVE_PEER_EVENT_TYPE_DELETE_CHUNK_REQUEST,
	SLAVE_PEER_EVENT_TYPE_DELETE_CHUNK_RESPONSE_SUCCESS,
	SLAVE_PEER_EVENT_TYPE_DELETE_CHUNK_RESPONSE_FAILURE,
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
			char *delta;
			volatile bool *status;
		} chunkUpdate;
		struct {
			size_t size;
			size_t index;
			SlaveProtocol *protocol;
		} send;
	} message;

	// Register
	void reqRegister( SlavePeerSocket *socket );
	void resRegister( SlavePeerSocket *socket, bool success = true );
	// UPDATE_CHUNK
	void reqUpdateChunk( SlavePeerSocket *socket, Metadata &metadata, uint32_t offset, uint32_t length, uint32_t updatingChunkId, volatile bool *status, char *delta );
	void resUpdateChunk( SlavePeerSocket *socket, Metadata &metadata, uint32_t offset, uint32_t length, uint32_t updatingChunkId, bool success );
	// DELETE_CHUNK
	void reqDeleteChunk( SlavePeerSocket *socket, Metadata &metadata, uint32_t offset, uint32_t length, uint32_t updatingChunkId, volatile bool *status, char *delta );
	void resDeleteChunk( SlavePeerSocket *socket, Metadata &metadata, uint32_t offset, uint32_t length, uint32_t updatingChunkId, bool success );
	// Pending
	void pending( SlavePeerSocket *socket );
};

#endif

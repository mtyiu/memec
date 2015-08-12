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
	SLAVE_PEER_EVENT_TYPE_UPDATE_CHUNK_RESPONSE_SUCCESS,
	SLAVE_PEER_EVENT_TYPE_UPDATE_CHUNK_RESPONSE_FAILURE,
	// DELETE_CHUNK
	SLAVE_PEER_EVENT_TYPE_DELETE_CHUNK_RESPONSE_SUCCESS,
	SLAVE_PEER_EVENT_TYPE_DELETE_CHUNK_RESPONSE_FAILURE,
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
	// UPDATE
	void resUpdateChunk( SlavePeerSocket *socket, Metadata &metadata, uint32_t offset, uint32_t length, bool success );
	// DELETE
	void resDeleteChunk( SlavePeerSocket *socket, Metadata &metadata, uint32_t offset, uint32_t length, bool success );
	// Send
	void send( SlavePeerSocket *socket, SlaveProtocol *protocol, size_t size, size_t index );
	// Pending
	void pending( SlavePeerSocket *socket );
};

#endif

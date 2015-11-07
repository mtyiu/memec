#ifndef __COORDINATOR_EVENT_SLAVE_EVENT_HH__
#define __COORDINATOR_EVENT_SLAVE_EVENT_HH__

#include "../socket/slave_socket.hh"
#include "../../common/event/event.hh"

enum SlaveEventType {
	SLAVE_EVENT_TYPE_UNDEFINED,
	SLAVE_EVENT_TYPE_REGISTER_RESPONSE_SUCCESS,
	SLAVE_EVENT_TYPE_REGISTER_RESPONSE_FAILURE,
	SLAVE_EVENT_TYPE_ANNOUNCE_SLAVE_CONNECTED,
	SLAVE_EVENT_TYPE_PENDING,
	SLAVE_EVENT_TYPE_REQUEST_SEAL_CHUNKS,
	SLAVE_EVENT_TYPE_REQUEST_FLUSH_CHUNKS,
	SLAVE_EVENT_TYPE_REQUEST_SYNC_META,
	SLAVE_EVENT_TYPE_DISCONNECT
};

class SlaveEvent : public Event {
public:
	SlaveEventType type;
	uint32_t id;
	SlaveSocket *socket;
	bool *sync;

	void pending( SlaveSocket *socket );
	void resRegister( SlaveSocket *socket, uint32_t id, bool success = true );
	void announceSlaveConnected( SlaveSocket *socket );
	void reqSealChunks( SlaveSocket *socket );
	void reqFlushChunks( SlaveSocket *socket );
	void reqSyncMeta( SlaveSocket *socket, bool *sync );
	void disconnect( SlaveSocket *socket );
};

#endif

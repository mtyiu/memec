#ifndef __SLAVE_EVENT_SLAVE_PEER_EVENT_HH__
#define __SLAVE_EVENT_SLAVE_PEER_EVENT_HH__

#include "../socket/slave_peer_socket.hh"
#include "../../common/event/event.hh"

enum SlavePeerEventType {
	SLAVE_PEER_EVENT_TYPE_UNDEFINED,
	SLAVE_PEER_EVENT_TYPE_REGISTER_REQUEST,
	SLAVE_PEER_EVENT_TYPE_REGISTER_RESPONSE_SUCCESS,
	SLAVE_PEER_EVENT_TYPE_REGISTER_RESPONSE_FAILURE,
	SLAVE_PEER_EVENT_TYPE_PENDING
};

class SlavePeerEvent : public Event {
public:
	SlavePeerEventType type;
	SlavePeerSocket *socket;

	void reqRegister( SlavePeerSocket *socket );
	void resRegister( SlavePeerSocket *socket, bool success = true );
	void pending( SlavePeerSocket *socket );
};

#endif

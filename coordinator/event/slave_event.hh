#ifndef __COORDINATOR_EVENT_SLAVE_EVENT_HH__
#define __COORDINATOR_EVENT_SLAVE_EVENT_HH__

#include "../socket/slave_socket.hh"
#include "../../common/event/event.hh"

enum SlaveEventType {
	SLAVE_EVENT_TYPE_UNDEFINED,
	SLAVE_EVENT_TYPE_REGISTER_RESPONSE_SUCCESS,
	SLAVE_EVENT_TYPE_REGISTER_RESPONSE_FAILURE,
	SLAVE_EVENT_TYPE_PENDING
};

class SlaveEvent : public Event {
public:
	SlaveEventType type;
	SlaveSocket *socket;

	void pending( SlaveSocket *socket );
	void resRegister( SlaveSocket *socket, bool success = true );
};

#endif

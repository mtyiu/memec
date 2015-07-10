#ifndef __COORDINATOR_EVENT_COORDINATOR_EVENT_HH__
#define __COORDINATOR_EVENT_COORDINATOR_EVENT_HH__

#include "../socket/coordinator_socket.hh"
#include "../../common/event/event.hh"

enum CoordinatorEventType {
	COORDINATOR_EVENT_TYPE_REGISTER_REQUEST,
	COORDINATOR_EVENT_TYPE_REGISTER_RESPONSE_SUCCESS,
	COORDINATOR_EVENT_TYPE_REGISTER_RESPONSE_FAILURE
};

class CoordinatorEvent : public Event {
public:
	CoordinatorEventType type;
	CoordinatorSocket *socket;

	void reqRegister( CoordinatorSocket *socket );
	void resRegister( CoordinatorSocket *socket, bool success );
};

#endif

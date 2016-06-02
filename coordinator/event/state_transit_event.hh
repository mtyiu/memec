#ifndef __COORDINATOR_EVENT_STATE_TRANSIT_STATE_EVENT_HH__
#define __COORDINATOR_EVENT_STATE_TRANSIT_STATE_EVENT_HH__

#include "../socket/coordinator_socket.hh"
#include "../../common/event/event.hh"
#include "../../common/ds/sockaddr_in.hh"

class StateTransitEvent : public Event<void> {
public:
	bool start;
	struct sockaddr_in server;
};

#endif

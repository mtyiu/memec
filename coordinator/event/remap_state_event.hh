#ifndef __COORDINATOR_EVENT_REMAP_STATE_EVENT_HH__
#define __COORDINATOR_EVENT_REMAP_STATE_EVENT_HH__

#include "../../common/event/event.hh"
#include "../../common/ds/sockaddr_in.hh"

class RemapStateEvent : public Event {
public:
	bool start;
	struct sockaddr_in server;
};

#endif

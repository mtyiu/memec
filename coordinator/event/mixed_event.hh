#ifndef __COORDINATOR_EVENT_MIXED_EVENT_HH__
#define __COORDINATOR_EVENT_MIXED_EVENT_HH__

#include "coordinator_event.hh"
#include "client_event.hh"
#include "server_event.hh"
#include "../../common/event/event.hh"
#include "../../common/event/event_type.hh"

class MixedEvent : public Event {
public:
	EventType type;
	union {
		CoordinatorEvent coordinator;
		ClientEvent client;
		ServerEvent server;
	} event;

#define MIXED_EVENT_SET(_EVENT_TYPE_, _TYPE_CONSTANT_, _FIELD_) \
	void set( _EVENT_TYPE_ &event ) { \
		this->type = _TYPE_CONSTANT_; \
		this->event._FIELD_ = event; \
	}

	MIXED_EVENT_SET( CoordinatorEvent, EVENT_TYPE_COORDINATOR, coordinator )
	MIXED_EVENT_SET( ClientEvent, EVENT_TYPE_CLIENT, client )
	MIXED_EVENT_SET( ServerEvent, EVENT_TYPE_SERVER, server )
#undef MIXED_EVENT_SET

	void set() {
		this->type = EVENT_TYPE_DUMMY;
	}
};

#endif

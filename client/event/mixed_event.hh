#ifndef __MASTER_EVENT_MIXED_EVENT_HH__
#define __MASTER_EVENT_MIXED_EVENT_HH__

#include "application_event.hh"
#include "coordinator_event.hh"
#include "master_event.hh"
#include "slave_event.hh"
#include "../../common/event/event.hh"
#include "../../common/event/event_type.hh"

class MixedEvent : public Event {
public:
	EventType type;
	union {
		ApplicationEvent application;
		CoordinatorEvent coordinator;
		MasterEvent master;
		SlaveEvent slave;
	} event;

#define MIXED_EVENT_SET(_EVENT_TYPE_, _TYPE_CONSTANT_, _FIELD_) \
	void set( _EVENT_TYPE_ &event ) { \
		this->type = _TYPE_CONSTANT_; \
		this->event._FIELD_ = event; \
	}

	MIXED_EVENT_SET( ApplicationEvent, EVENT_TYPE_APPLICATION, application )
	MIXED_EVENT_SET( CoordinatorEvent, EVENT_TYPE_COORDINATOR, coordinator )
	MIXED_EVENT_SET( MasterEvent, EVENT_TYPE_CLIENT, master )
	MIXED_EVENT_SET( SlaveEvent, EVENT_TYPE_SERVER, slave )
#undef MIXED_EVENT_SET

	void set() {
		this->type = EVENT_TYPE_DUMMY;
	}
};

#endif

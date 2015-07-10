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
};

#endif

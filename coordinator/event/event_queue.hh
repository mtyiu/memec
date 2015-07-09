#ifndef __COORDINATOR_EVENT_EVENT_QUEUE_HH__
#define __COORDINATOR_EVENT_EVENT_QUEUE_HH__

#include "../event/mixed_event.hh"
#include "../event/application_event.hh"
#include "../event/coordinator_event.hh"
#include "../event/master_event.hh"
#include "../event/slave_event.hh"
#include "../../common/event/event_queue.hh"

typedef union {
	EventQueue<MixedEvent> *mixed;
	struct {
		EventQueue<ApplicationEvent> *application;
		EventQueue<CoordinatorEvent> *coordinator;
		EventQueue<MasterEvent> *master;
		EventQueue<SlaveEvent> *slave;
	} separated;
} CoordinatorEventQueue;

#endif

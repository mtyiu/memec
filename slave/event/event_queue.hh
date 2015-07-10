#ifndef __SLAVE_EVENT_EVENT_QUEUE_HH__
#define __SLAVE_EVENT_EVENT_QUEUE_HH__

#include "mixed_event.hh"
#include "application_event.hh"
#include "coordinator_event.hh"
#include "master_event.hh"
#include "slave_event.hh"
#include "../../common/event/event_queue.hh"

typedef union {
	EventQueue<MixedEvent> *mixed;
	struct {
		EventQueue<ApplicationEvent> *application;
		EventQueue<CoordinatorEvent> *coordinator;
		EventQueue<MasterEvent> *master;
		EventQueue<SlaveEvent> *slave;
	} separated;
} SlaveEventQueue;

#endif

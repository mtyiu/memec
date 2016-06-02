#ifndef __COORDINATOR_EVENT_COORDINATOR_EVENT_QUEUE_HH__
#define __COORDINATOR_EVENT_COORDINATOR_EVENT_QUEUE_HH__

#include "mixed_event.hh"
#include "coordinator_event.hh"
#include "client_event.hh"
#include "server_event.hh"
#include "../../common/event/event_queue.hh"

class CoordinatorEventQueue : public EventQueue<MixedEvent> {
public:
	DEFINE_EVENT_QUEUE_INSERT( CoordinatorEvent )
	DEFINE_EVENT_QUEUE_INSERT( ClientEvent )
	DEFINE_EVENT_QUEUE_INSERT( ServerEvent )

	DEFINE_EVENT_QUEUE_PRIORITIZED_INSERT( ClientEvent )
};

#endif

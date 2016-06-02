#ifndef __CLIENT_EVENT_CLIENT_EVENT_QUEUE_HH__
#define __CLIENT_EVENT_CLIENT_EVENT_QUEUE_HH__

#include "mixed_event.hh"
#include "application_event.hh"
#include "coordinator_event.hh"
#include "server_event.hh"
#include "../../common/event/event_queue.hh"

class ClientEventQueue : public EventQueue<MixedEvent> {
public:
	DEFINE_EVENT_QUEUE_INSERT( ApplicationEvent )
	DEFINE_EVENT_QUEUE_INSERT( CoordinatorEvent )
	DEFINE_EVENT_QUEUE_INSERT( ServerEvent )

	DEFINE_EVENT_QUEUE_PRIORITIZED_INSERT( ServerEvent )
};

#endif

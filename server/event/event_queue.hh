#ifndef __SERVER_EVENT_SERVER_EVENT_QUEUE_HH__
#define __SERVER_EVENT_SERVER_EVENT_QUEUE_HH__

#include "mixed_event.hh"
#include "coding_event.hh"
#include "coordinator_event.hh"
#include "io_event.hh"
#include "client_event.hh"
#include "server_peer_event.hh"
#include "../../common/event/event_queue.hh"

class ServerEventQueue : public EventQueue<MixedEvent> {
public:
	DEFINE_EVENT_QUEUE_INSERT( CodingEvent )
	DEFINE_EVENT_QUEUE_INSERT( CoordinatorEvent )
	DEFINE_EVENT_QUEUE_INSERT( IOEvent )
	DEFINE_EVENT_QUEUE_INSERT( ClientEvent )
	DEFINE_EVENT_QUEUE_INSERT( ServerPeerEvent )

	DEFINE_EVENT_QUEUE_PRIORITIZED_INSERT( ServerPeerEvent )
};

#endif

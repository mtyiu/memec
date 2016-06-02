#ifndef __APPLICATION_EVENT_APPLICATION_EVENT_QUEUE_HH__
#define __APPLICATION_EVENT_APPLICATION_EVENT_QUEUE_HH__

#include "mixed_event.hh"
#include "client_event.hh"
#include "../../common/event/event_queue.hh"

class ApplicationEventQueue : public EventQueue<MixedEvent> {
public:
	DEFINE_EVENT_QUEUE_INSERT( ClientEvent )
};

#endif

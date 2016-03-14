#ifndef __APPLICATION_EVENT_MIXED_EVENT_HH__
#define __APPLICATION_EVENT_MIXED_EVENT_HH__

#include "application_event.hh"
#include "client_event.hh"
#include "../../common/event/event.hh"
#include "../../common/event/event_type.hh"

class MixedEvent : public Event {
public:
	EventType type;
	union {
		ApplicationEvent application;
		ClientEvent client;
	} event;

#define MIXED_EVENT_SET(_EVENT_TYPE_, _TYPE_CONSTANT_, _FIELD_) \
	void set( _EVENT_TYPE_ &event ) { \
		this->type = _TYPE_CONSTANT_; \
		this->event._FIELD_ = event; \
	}

	MIXED_EVENT_SET( ApplicationEvent, EVENT_TYPE_APPLICATION, application )
	MIXED_EVENT_SET( ClientEvent, EVENT_TYPE_CLIENT, client )
#undef MIXED_EVENT_SET
};

#endif

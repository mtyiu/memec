#ifndef __SERVER_EVENT_MIXED_EVENT_HH__
#define __SERVER_EVENT_MIXED_EVENT_HH__

#include "coding_event.hh"
#include "coordinator_event.hh"
#include "io_event.hh"
#include "client_event.hh"
#include "server_peer_event.hh"
#include "../../common/event/event.hh"
#include "../../common/event/event_type.hh"

class MixedEvent {
public:
	EventType type;
	union {
		CodingEvent coding;
		CoordinatorEvent coordinator;
		IOEvent io;
		ClientEvent client;
		ServerPeerEvent serverPeer;
	} event;

#define MIXED_EVENT_SET(_EVENT_TYPE_, _TYPE_CONSTANT_, _FIELD_) \
	void set( _EVENT_TYPE_ &event ) { \
		this->type = _TYPE_CONSTANT_; \
		this->event._FIELD_ = event; \
	}

	MIXED_EVENT_SET( CodingEvent, EVENT_TYPE_CODING, coding )
	MIXED_EVENT_SET( CoordinatorEvent, EVENT_TYPE_COORDINATOR, coordinator )
	MIXED_EVENT_SET( IOEvent, EVENT_TYPE_IO, io )
	MIXED_EVENT_SET( ClientEvent, EVENT_TYPE_CLIENT, client )
	MIXED_EVENT_SET( ServerPeerEvent, EVENT_TYPE_SERVER_PEER, serverPeer )
#undef MIXED_EVENT_SET

	void set() {
		this->type = EVENT_TYPE_DUMMY;
	}
};

#endif

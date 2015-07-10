#ifndef __SLAVE_EVENT_MIXED_EVENT_HH__
#define __SLAVE_EVENT_MIXED_EVENT_HH__

#include "coordinator_event.hh"
#include "master_event.hh"
#include "slave_event.hh"
#include "slave_peer_event.hh"
#include "../../common/event/event.hh"
#include "../../common/event/event_type.hh"

class MixedEvent : public Event {
public:
	EventType type;
	union {
		CoordinatorEvent coordinator;
		MasterEvent master;
		SlaveEvent slave;
		SlavePeerEvent slavePeer;
	} event;

#define MIXED_EVENT_SET(_EVENT_TYPE_, _TYPE_CONSTANT_, _FIELD_) \
	void set( _EVENT_TYPE_ &event ) { \
		this->type = _TYPE_CONSTANT_; \
		this->event._FIELD_ = event; \
	}

	MIXED_EVENT_SET( CoordinatorEvent, EVENT_TYPE_COORDINATOR, coordinator )
	MIXED_EVENT_SET( MasterEvent, EVENT_TYPE_MASTER, master )
	MIXED_EVENT_SET( SlaveEvent, EVENT_TYPE_SLAVE, slave )
	MIXED_EVENT_SET( SlavePeerEvent, EVENT_TYPE_SLAVE_PEER, slavePeer )
#undef MIXED_EVENT_SET
};

#endif

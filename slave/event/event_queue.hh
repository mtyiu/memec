#ifndef __SLAVE_EVENT_SLAVE_EVENT_QUEUE_HH__
#define __SLAVE_EVENT_SLAVE_EVENT_QUEUE_HH__

#include <stdint.h>
#include "mixed_event.hh"
#include "coordinator_event.hh"
#include "master_event.hh"
#include "slave_event.hh"
#include "slave_peer_event.hh"
#include "../../common/event/event_queue.hh"

class SlaveEventQueue {
public:
	bool isMixed;
	EventQueue<MixedEvent> *mixed;
	struct {
		EventQueue<CoordinatorEvent> *coordinator;
		EventQueue<MasterEvent> *master;
		EventQueue<SlaveEvent> *slave;
		EventQueue<SlavePeerEvent> *slavePeer;
	} separated;

	SlaveEventQueue() {
		this->mixed = 0;
		this->separated.coordinator = 0;
		this->separated.master = 0;
		this->separated.slave = 0;
		this->separated.slavePeer = 0;
	}

	void init( bool block, uint32_t mixed ) {
		this->isMixed = true;
		this->mixed = new EventQueue<MixedEvent>( mixed, block );
	}

	void init( bool block, uint32_t coordinator, uint32_t master, uint32_t slave, uint32_t slavePeer ) {
		this->isMixed = false;
		this->separated.coordinator = new EventQueue<CoordinatorEvent>( coordinator, block );
		this->separated.master = new EventQueue<MasterEvent>( master, block );
		this->separated.slave = new EventQueue<SlaveEvent>( slave, block );
		this->separated.slavePeer = new EventQueue<SlavePeerEvent>( slave, block );
	}

	void start() {
		if ( this->isMixed ) {
			this->mixed->start();
		} else {
			this->separated.coordinator->start();
			this->separated.master->start();
			this->separated.slave->start();
			this->separated.slavePeer->start();
		}
	}

	void stop() {
		if ( this->isMixed ) {
			this->mixed->stop();
		} else {
			this->separated.coordinator->stop();
			this->separated.master->stop();
			this->separated.slave->stop();
			this->separated.slavePeer->stop();
		}
	}

	void free() {
		if ( this->isMixed ) {
			delete this->mixed;
		} else {
			delete this->separated.coordinator;
			delete this->separated.master;
			delete this->separated.slave;
			delete this->separated.slavePeer;
		}
	}

#define SLAVE_EVENT_QUEUE_INSERT(_EVENT_TYPE_, _EVENT_QUEUE_) \
	bool insert( _EVENT_TYPE_ &event ) { \
		if ( this->isMixed ) { \
			MixedEvent mixedEvent; \
			mixedEvent.set( event ); \
			return this->mixed->insert( mixedEvent ); \
		} else { \
			return this->separated._EVENT_QUEUE_->insert( event ); \
		} \
	}

	SLAVE_EVENT_QUEUE_INSERT( CoordinatorEvent, coordinator )
	SLAVE_EVENT_QUEUE_INSERT( MasterEvent, master )
	SLAVE_EVENT_QUEUE_INSERT( SlaveEvent, slave )
	SLAVE_EVENT_QUEUE_INSERT( SlavePeerEvent, slavePeer )
#undef SLAVE_EVENT_QUEUE_INSERT
};

#endif

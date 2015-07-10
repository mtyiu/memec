#ifndef __COORDINATOR_EVENT_COORDINATOR_EVENT_QUEUE_HH__
#define __COORDINATOR_EVENT_COORDINATOR_EVENT_QUEUE_HH__

#include <stdint.h>
#include "mixed_event.hh"
#include "application_event.hh"
#include "coordinator_event.hh"
#include "master_event.hh"
#include "slave_event.hh"
#include "../../common/event/event_queue.hh"

class CoordinatorEventQueue {
public:
	bool isMixed;
	EventQueue<MixedEvent> *mixed;
	struct {
		EventQueue<ApplicationEvent> *application;
		EventQueue<CoordinatorEvent> *coordinator;
		EventQueue<MasterEvent> *master;
		EventQueue<SlaveEvent> *slave;
	} separated;

	CoordinatorEventQueue() {
		this->mixed = 0;
		this->separated.application = 0;
		this->separated.coordinator = 0;
		this->separated.master = 0;
		this->separated.slave = 0;
	}

	void init( bool block, uint32_t mixed ) {
		this->isMixed = true;
		this->mixed = new EventQueue<MixedEvent>( mixed, block );
	}

	void init( bool block, uint32_t application, uint32_t coordinator, uint32_t master, uint32_t slave ) {
		this->isMixed = false;
		this->separated.application = new EventQueue<ApplicationEvent>( application, block );
		this->separated.coordinator = new EventQueue<CoordinatorEvent>( coordinator, block );
		this->separated.master = new EventQueue<MasterEvent>( master, block );
		this->separated.slave = new EventQueue<SlaveEvent>( slave, block );
	}

	void start() {
		if ( this->isMixed ) {
			this->mixed->start();
		} else {
			this->separated.application->start();
			this->separated.coordinator->start();
			this->separated.master->start();
			this->separated.slave->start();
		}
	}

	void stop() {
		if ( this->isMixed ) {
			this->mixed->stop();
		} else {
			this->separated.application->stop();
			this->separated.coordinator->stop();
			this->separated.master->stop();
			this->separated.slave->stop();
		}
	}

	void free() {
		if ( this->isMixed ) {
			delete this->mixed;
		} else {
			delete this->separated.application;
			delete this->separated.coordinator;
			delete this->separated.master;
			delete this->separated.slave;
		}
	}

#define COORDINATOR_EVENT_QUEUE_INSERT(_EVENT_TYPE_, _EVENT_QUEUE_) \
	bool insert( _EVENT_TYPE_ &event ) { \
		if ( this->isMixed ) { \
			MixedEvent mixedEvent; \
			mixedEvent.set( event ); \
			return this->mixed->insert( mixedEvent ); \
		} else { \
			return this->separated._EVENT_QUEUE_->insert( event ); \
		} \
	}

	COORDINATOR_EVENT_QUEUE_INSERT( ApplicationEvent, application )
	COORDINATOR_EVENT_QUEUE_INSERT( CoordinatorEvent, coordinator )
	COORDINATOR_EVENT_QUEUE_INSERT( MasterEvent, master )
	COORDINATOR_EVENT_QUEUE_INSERT( SlaveEvent, slave )
#undef COORDINATOR_EVENT_QUEUE_INSERT
};

#endif

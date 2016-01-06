#ifndef __MASTER_EVENT_MASTER_EVENT_QUEUE_HH__
#define __MASTER_EVENT_MASTER_EVENT_QUEUE_HH__

#include <stdint.h>
#include "mixed_event.hh"
#include "application_event.hh"
#include "coordinator_event.hh"
#include "master_event.hh"
#include "slave_event.hh"
#include "../../common/event/event_queue.hh"
#include "../../common/lock/lock.hh"

class MasterEventQueue {
public:
	bool isMixed;
	EventQueue<MixedEvent> *mixed;
	struct {
		 // High priority
		EventQueue<MixedEvent> *mixed;
		LOCK_T lock;
		uint32_t capacity;
		uint32_t count;
	} priority;
	struct {
		EventQueue<ApplicationEvent> *application;
		EventQueue<CoordinatorEvent> *coordinator;
		EventQueue<MasterEvent> *master;
		EventQueue<SlaveEvent> *slave;
	} separated;

	MasterEventQueue() {
		this->mixed = 0;
		this->priority.mixed = 0;
		this->priority.capacity = 0;
		this->priority.count = 0;
		this->separated.application = 0;
		this->separated.coordinator = 0;
		this->separated.master = 0;
		this->separated.slave = 0;
	}

	void init( bool block, uint32_t mixed, uint32_t pMixed ) {
		this->isMixed = true;
		this->mixed = new EventQueue<MixedEvent>( mixed, block );
		this->priority.mixed = new EventQueue<MixedEvent>( pMixed, false );
		this->priority.capacity = pMixed;
		LOCK_INIT( &this->priority.lock );
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
			this->priority.mixed->start();
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
			this->priority.mixed->stop();
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
			delete this->priority.mixed;
		} else {
			delete this->separated.application;
			delete this->separated.coordinator;
			delete this->separated.master;
			delete this->separated.slave;
		}
	}

	void print( FILE *f = stdout ) {
		if ( this->isMixed ) {
			fprintf( f, "[Mixed] " );
			this->mixed->print( f );
			fprintf( f, "[Mixed (Prioritized)] " );
			this->priority.mixed->print( f );
		} else {
			fprintf( f, "[Application] " );
			this->separated.application->print( f );
			fprintf( f, "[Coordinator] " );
			this->separated.coordinator->print( f );
			fprintf( f, "[     Master] " );
			this->separated.master->print( f );
			fprintf( f, "[      Slave] " );
			this->separated.slave->print( f );
		}
	}

#define MASTER_EVENT_QUEUE_INSERT(_EVENT_TYPE_, _EVENT_QUEUE_) \
	bool insert( _EVENT_TYPE_ &event ) { \
		if ( this->isMixed ) { \
			MixedEvent mixedEvent; \
			mixedEvent.set( event ); \
			bool ret = this->mixed->insert( mixedEvent ); \
			return ret; \
		} else { \
			return this->separated._EVENT_QUEUE_->insert( event ); \
		} \
	}

	MASTER_EVENT_QUEUE_INSERT( ApplicationEvent, application )
	MASTER_EVENT_QUEUE_INSERT( CoordinatorEvent, coordinator )
	MASTER_EVENT_QUEUE_INSERT( MasterEvent, master )
	MASTER_EVENT_QUEUE_INSERT( SlaveEvent, slave )
#undef MASTER_EVENT_QUEUE_INSERT

	bool prioritizedInsert( SlaveEvent &event ) {
		bool ret;
		if ( this->isMixed ) {
			MixedEvent mixedEvent;
			mixedEvent.set( event );
			if ( this->mixed->count() && LOCK( &this->priority.lock ) == 0 ) {
				// Locked
				if ( this->priority.count < this->priority.capacity ) {
					this->priority.count++;
					ret = this->priority.mixed->insert( mixedEvent );
					UNLOCK( &this->priority.lock );

					// Avoid all worker threads are blocked by the empty normal queue
					if ( this->mixed->count() == 0 ) {
						mixedEvent.set();
						this->mixed->insert( mixedEvent );
					}

					return ret;
				} else {
					UNLOCK( &this->priority.lock );
					return this->mixed->insert( mixedEvent );
				}
			} else {
				ret = this->mixed->insert( mixedEvent );
				return ret;
			}
		} else {
			return this->separated.slave->insert( event );
		}
	}

	bool extractMixed( MixedEvent &event ) {
		if ( this->priority.mixed->extract( event ) ) {
			LOCK( &this->priority.lock );
			this->priority.count--;
			UNLOCK( &this->priority.lock );
			return true;
		} else {
			return this->mixed->extract( event );
		}
	}
};

#endif

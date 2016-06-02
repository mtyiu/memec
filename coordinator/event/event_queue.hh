#ifndef __COORDINATOR_EVENT_COORDINATOR_EVENT_QUEUE_HH__
#define __COORDINATOR_EVENT_COORDINATOR_EVENT_QUEUE_HH__

#include <stdint.h>
#include "mixed_event.hh"
#include "coordinator_event.hh"
#include "client_event.hh"
#include "server_event.hh"
#include "../../common/event/event_queue.hh"

class CoordinatorEventQueue {
public:
	EventQueue<MixedEvent> *mixed;
	struct {
		 // High priority
		EventQueue<MixedEvent> *mixed;
		LOCK_T lock;
		uint32_t capacity;
		uint32_t count;
	} priority;

	CoordinatorEventQueue() {
		this->priority.mixed = 0;
		this->priority.capacity = 0;
		this->priority.count = 0;
	}

	void init( bool block, uint32_t mixed, uint32_t pMixed ) {
		this->mixed = new EventQueue<MixedEvent>( mixed, block );
		this->priority.mixed = new EventQueue<MixedEvent>( pMixed, false );
		this->priority.capacity = pMixed;
		LOCK_INIT( &this->priority.lock );
	}

	void start() {
		this->mixed->start();
		this->priority.mixed->start();
	}

	void stop() {
		this->mixed->stop();
		this->priority.mixed->stop();
	}

	void free() {
		delete this->mixed;
		delete this->priority.mixed;
	}

	void print( FILE *f = stdout ) {
		fprintf( f, "[Mixed] " );
		this->mixed->print( f );
		fprintf( f, "[Mixed (Prioritized)] " );
		this->priority.mixed->print( f );
	}

#define COORDINATOR_EVENT_QUEUE_INSERT(_EVENT_TYPE_, _EVENT_QUEUE_) \
	bool insert( _EVENT_TYPE_ &event ) { \
		MixedEvent mixedEvent; \
		mixedEvent.set( event ); \
		return this->mixed->insert( mixedEvent ); \
	}

	COORDINATOR_EVENT_QUEUE_INSERT( CoordinatorEvent, coordinator )
	COORDINATOR_EVENT_QUEUE_INSERT( ClientEvent, client )
	COORDINATOR_EVENT_QUEUE_INSERT( ServerEvent, server )
#undef COORDINATOR_EVENT_QUEUE_INSERT

	bool prioritizedInsert( ClientEvent &event ) {
		bool ret;
		MixedEvent mixedEvent;
		mixedEvent.set( event );
		size_t count, size;
		count = ( size_t ) this->mixed->count( &size );
		if ( count && TRY_LOCK( &this->priority.lock ) == 0 ) {
			// Locked
			if ( this->priority.count < this->priority.capacity ) {
				this->priority.count++;
				ret = this->priority.mixed->insert( mixedEvent );
				UNLOCK( &this->priority.lock );

				// Avoid all worker threads are blocked by the empty normal queue
				if ( this->mixed->count() < ( int ) count ) {
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

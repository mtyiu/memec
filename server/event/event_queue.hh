#ifndef __SERVER_EVENT_SERVER_EVENT_QUEUE_HH__
#define __SERVER_EVENT_SERVER_EVENT_QUEUE_HH__

#include <stdint.h>
#include "mixed_event.hh"
#include "coding_event.hh"
#include "coordinator_event.hh"
#include "io_event.hh"
#include "client_event.hh"
#include "server_peer_event.hh"
#include "../../common/event/event_queue.hh"
#include "../../common/lock/lock.hh"

class ServerEventQueue {
public:
	EventQueue<MixedEvent> *mixed;
	struct {
		 // High priority
		EventQueue<MixedEvent> *mixed;
		LOCK_T lock;
		uint32_t capacity;
		uint32_t count;
	} priority;

	ServerEventQueue() {
		this->mixed = 0;
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

#define SERVER_EVENT_QUEUE_INSERT(_EVENT_TYPE_, _EVENT_QUEUE_) \
	bool insert( _EVENT_TYPE_ &event ) { \
		MixedEvent mixedEvent; \
		mixedEvent.set( event ); \
		return this->mixed->insert( mixedEvent ); \
	}

	SERVER_EVENT_QUEUE_INSERT( CodingEvent, coding )
	SERVER_EVENT_QUEUE_INSERT( CoordinatorEvent, coordinator )
	SERVER_EVENT_QUEUE_INSERT( IOEvent, io )
	SERVER_EVENT_QUEUE_INSERT( ClientEvent, client )
	SERVER_EVENT_QUEUE_INSERT( ServerPeerEvent, serverPeer )
#undef SERVER_EVENT_QUEUE_INSERT

	bool prioritizedInsert( ServerPeerEvent &event ) {
		MixedEvent mixedEvent;
		mixedEvent.set( event );
		size_t count, size;
		count = ( size_t ) this->mixed->count( &size );
		if ( count && TRY_LOCK( &this->priority.lock ) == 0 ) {
			// Locked
			if ( this->priority.count < this->priority.capacity ) {
				this->priority.count++;
				bool ret = this->priority.mixed->insert( mixedEvent );
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
			return this->mixed->insert( mixedEvent );
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

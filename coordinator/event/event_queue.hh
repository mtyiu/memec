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
		EventQueue<CoordinatorEvent> *coordinator;
		EventQueue<ClientEvent> *client;
		EventQueue<ServerEvent> *server;
	} separated;

	CoordinatorEventQueue() {
		this->mixed = 0;
		this->priority.mixed = 0;
		this->priority.capacity = 0;
		this->priority.count = 0;
		this->separated.coordinator = 0;
		this->separated.client = 0;
		this->separated.server = 0;
	}

	void init( bool block, uint32_t mixed, uint32_t pMixed ) {
		this->isMixed = true;
		this->mixed = new EventQueue<MixedEvent>( mixed, block );
		this->priority.mixed = new EventQueue<MixedEvent>( pMixed, false );
		this->priority.capacity = pMixed;
		LOCK_INIT( &this->priority.lock );
	}

	void init( bool block, uint32_t coordinator, uint32_t client, uint32_t server ) {
		this->isMixed = false;
		this->separated.coordinator = new EventQueue<CoordinatorEvent>( coordinator, block );
		this->separated.client = new EventQueue<ClientEvent>( client, block );
		this->separated.server = new EventQueue<ServerEvent>( server, block );
	}

	void start() {
		if ( this->isMixed ) {
			this->mixed->start();
			this->priority.mixed->start();
		} else {
			this->separated.coordinator->start();
			this->separated.client->start();
			this->separated.server->start();
		}
	}

	void stop() {
		if ( this->isMixed ) {
			this->mixed->stop();
			this->priority.mixed->stop();
		} else {
			this->separated.coordinator->stop();
			this->separated.client->stop();
			this->separated.server->stop();
		}
	}

	void free() {
		if ( this->isMixed ) {
			delete this->mixed;
			delete this->priority.mixed;
		} else {
			delete this->separated.coordinator;
			delete this->separated.client;
			delete this->separated.server;
		}
	}

	void print( FILE *f = stdout ) {
		if ( this->isMixed ) {
			fprintf( f, "[Mixed] " );
			this->mixed->print( f );
			fprintf( f, "[Mixed (Prioritized)] " );
			this->priority.mixed->print( f );
		} else {
			fprintf( f, "[Coordinator] " );
			this->separated.coordinator->print( f );
			fprintf( f, "[     Client] " );
			this->separated.client->print( f );
			fprintf( f, "[      Server] " );
			this->separated.server->print( f );
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

	COORDINATOR_EVENT_QUEUE_INSERT( CoordinatorEvent, coordinator )
	COORDINATOR_EVENT_QUEUE_INSERT( ClientEvent, client )
	COORDINATOR_EVENT_QUEUE_INSERT( ServerEvent, server )
#undef COORDINATOR_EVENT_QUEUE_INSERT

	bool prioritizedInsert( ClientEvent &event ) {
		bool ret;
		if ( this->isMixed ) {
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
		} else {
			return this->separated.client->insert( event );
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

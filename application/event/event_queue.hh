#ifndef __APPLICATION_EVENT_MASTER_EVENT_QUEUE_HH__
#define __APPLICATION_EVENT_MASTER_EVENT_QUEUE_HH__

#include <stdint.h>
#include "mixed_event.hh"
#include "application_event.hh"
#include "master_event.hh"
#include "../../common/event/event_queue.hh"

class ApplicationEventQueue {
public:
	bool isMixed;
	EventQueue<MixedEvent> *mixed;
	struct {
		EventQueue<ApplicationEvent> *application;
		EventQueue<MasterEvent> *master;
	} separated;

	ApplicationEventQueue() {
		this->mixed = 0;
		this->separated.application = 0;
		this->separated.master = 0;
	}

	void init( bool block, uint32_t mixed ) {
		this->isMixed = true;
		this->mixed = new EventQueue<MixedEvent>( mixed, block );
	}

	void init( bool block, uint32_t application, uint32_t master ) {
		this->isMixed = false;
		this->separated.application = new EventQueue<ApplicationEvent>( application, block );
		this->separated.master = new EventQueue<MasterEvent>( master, block );
	}

	void start() {
		if ( this->isMixed ) {
			this->mixed->start();
		} else {
			this->separated.application->start();
			this->separated.master->start();
		}
	}

	void stop() {
		if ( this->isMixed ) {
			this->mixed->stop();
		} else {
			this->separated.application->stop();
			this->separated.master->stop();
		}
	}

	void free() {
		if ( this->isMixed ) {
			delete this->mixed;
		} else {
			delete this->separated.application;
			delete this->separated.master;
		}
	}

	void print( FILE *f = stdout ) {
		if ( this->isMixed ) {
			fprintf( f, "[Mixed] " );
			this->mixed->print( f );
		} else {
			fprintf( f, "[Application] " );
			this->separated.application->print( f );
			fprintf( f, "[     Master] " );
			this->separated.master->print( f );
		}
	}

#define APPLICATION_EVENT_QUEUE_INSERT(_EVENT_TYPE_, _EVENT_QUEUE_) \
	bool insert( _EVENT_TYPE_ &event ) { \
		if ( this->isMixed ) { \
			MixedEvent mixedEvent; \
			mixedEvent.set( event ); \
			return this->mixed->insert( mixedEvent ); \
		} else { \
			return this->separated._EVENT_QUEUE_->insert( event ); \
		} \
	}

	APPLICATION_EVENT_QUEUE_INSERT( ApplicationEvent, application )
	APPLICATION_EVENT_QUEUE_INSERT( MasterEvent, master )
#undef APPLICATION_EVENT_QUEUE_INSERT
};

#endif

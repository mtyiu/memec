#ifndef __APPLICATION_EVENT_APPLICATION_EVENT_QUEUE_HH__
#define __APPLICATION_EVENT_APPLICATION_EVENT_QUEUE_HH__

#include <stdint.h>
#include "mixed_event.hh"
#include "client_event.hh"
#include "../../common/event/event_queue.hh"

class ApplicationEventQueue {
public:
	EventQueue<MixedEvent> *mixed;

	ApplicationEventQueue() {
		this->mixed = 0;
	}

	void init( bool block, uint32_t size ) {
		this->mixed = new EventQueue<MixedEvent>( size, block );
	}

	void start() {
		this->mixed->start();
	}

	void stop() {
		this->mixed->stop();
	}

	void free() {
		delete this->mixed;
	}

	void print( FILE *f = stdout ) {
		fprintf( f, "[Mixed] " );
		this->mixed->print( f );
	}

#define APPLICATION_EVENT_QUEUE_INSERT(_EVENT_TYPE_) \
	bool insert( _EVENT_TYPE_ &event ) { \
		MixedEvent mixedEvent; \
		mixedEvent.set( event ); \
		return this->mixed->insert( mixedEvent ); \
	}

	APPLICATION_EVENT_QUEUE_INSERT( ClientEvent )
#undef APPLICATION_EVENT_QUEUE_INSERT
};

#endif

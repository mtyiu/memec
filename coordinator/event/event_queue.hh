#ifndef __COORDINATOR_EVENT_EVENT_QUEUE_HH_
#define __COORDINATOR_EVENT_EVENT_QUEUE_HH_

#include "mixed_event.hh"
#include "application_event.hh"
#include "coordinator_event.hh"
#include "master_event.hh"
#include "slave_event.hh"
#include "../../common/event/event_queue.hh"
#include "../../common/ds/ring_buffer.hh"

class CoordinatorEventQueue : public EventQueue {
private:
	struct {
		size_t size;
		EventType type;
		bool block;
	} config;
	bool isRunning;
	union {
		RingBuffer<MixedEvent> *mixed;
		RingBuffer<ApplicationEvent> *application;
		RingBuffer<CoordinatorEvent> *coordinator;
		RingBuffer<MasterEvent> *master;
		RingBuffer<SlaveEvent> *slave;
	} events;

	void free();

public:
	CoordinatorEventQueue();
	bool init( size_t size, EventType type, bool lock = true, bool block = true );
	bool start();
	void stop();
	void info( FILE *f = stdout );
	void debug( FILE *f = stdout );

	bool insert( MasterEvent event );

	bool extract( MixedEvent &event );
	bool extract( MasterEvent &event );
};

#endif

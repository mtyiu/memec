#include <cstdio>
#include "event_queue.hh"

void CoordinatorEventQueue::free() {
	switch( this->config.type ) {
		case EVENT_TYPE_MIXED:
			delete this->events.mixed;
			break;
		case EVENT_TYPE_APPLICATION:
			delete this->events.application;
			break;
		case EVENT_TYPE_COORDINATOR:
			delete this->events.coordinator;
			break;
		case EVENT_TYPE_MASTER:
			delete this->events.master;
			break;
		case EVENT_TYPE_SLAVE:
			delete this->events.slave;
			break;
		default:
			break;
	}
}

CoordinatorEventQueue::CoordinatorEventQueue() {
	this->config.size = 0;
	this->config.type = EVENT_TYPE_UNDEFINED;
	this->isRunning = false;
 }

bool CoordinatorEventQueue::init( size_t size, EventType type, bool lock, bool block ) {
	switch( type ) {
		case EVENT_TYPE_MIXED:
			this->events.mixed = new RingBuffer<MixedEvent>( size, block );
			break;
		case EVENT_TYPE_APPLICATION:
			this->events.application = new RingBuffer<ApplicationEvent>( size, block );
			break;
		case EVENT_TYPE_COORDINATOR:
			this->events.coordinator = new RingBuffer<CoordinatorEvent>( size, block );
			break;
		case EVENT_TYPE_MASTER:
			this->events.master = new RingBuffer<MasterEvent>( size, block );
			break;
		case EVENT_TYPE_SLAVE:
			this->events.slave = new RingBuffer<SlaveEvent>( size, block );
			break;
		default:
			return false;
	}
	this->config.size = size;
	this->config.type = type;
	this->config.block = block;
	return true;
}

bool CoordinatorEventQueue::start() {
	this->isRunning = true;
	return true;
}

void CoordinatorEventQueue::stop() {
	if ( ! this->isRunning )
		return;

	this->isRunning = false;
	this->free();
}

void CoordinatorEventQueue::info( FILE *f ) {

}

void CoordinatorEventQueue::debug( FILE *f ) {

}

#ifndef __COMMON_EVENT_EVENT_QUEUE_HH__
#define __COMMON_EVENT_EVENT_QUEUE_HH__

#include <cstdio>
#include "../ds/ring_buffer.hh"

template<class EventType> class EventQueue {
protected:
	struct {
		size_t size;
		bool block;
	} config;
	bool isRunning;
	RingBuffer<EventType> *queue;

	void free() {
		delete this->queue;
	}

public:
	EventQueue( size_t size, bool block = true ) {
		this->config.size = size;
		this->config.block = block;
		this->isRunning = false;
		this->queue = new RingBuffer<EventType>( size, block );
	}

	bool start() {
		this->isRunning = isRunning;
		return true;
	}

	void stop() {
		if ( ! this->isRunning )
			return;

		this->isRunning = false;
		this->free();
	}

	void debug( FILE *f = stdout ) {
		fprintf( f, "%d / %u", this->queue->GetCount(), this->size );
	}

	bool insert( EventType event ) {
		return this->queue->Insert( &event, sizeof( EventType ) ) == 0;
	}

	bool extract( EventType &event ) {
		return this->queue->Extract( &event ) == 0;
	}
};

#endif

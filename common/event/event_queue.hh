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

public:
	EventQueue( size_t size, bool block = true ) {
		this->config.size = size;
		this->config.block = block;
		this->isRunning = false;
		this->queue = new RingBuffer<EventType>( size, block );
	}

	~EventQueue() {
		delete this->queue;
	}

	bool start() {
		this->isRunning = true;
		return true;
	}

	void stop() {
		if ( ! this->isRunning )
			return;
		this->queue->Stop();
		this->isRunning = false;
	}

	void print( FILE *f = stdout ) {
		fprintf( f, "%d / %lu\n", this->queue->GetCount(), this->config.size );
	}

	bool insert( EventType &event ) {
		if ( this->isRunning )
			return this->queue->Insert( &event, sizeof( EventType ) ) == 0;
		return false;
	}

	bool extract( EventType &event ) {
		bool ret;
		ret = this->queue->Extract( &event ) == 0;
		return ret;
	}
};

#endif

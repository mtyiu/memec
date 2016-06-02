#ifndef __COMMON_EVENT_EVENT_QUEUE_HH__
#define __COMMON_EVENT_EVENT_QUEUE_HH__

#include <cstdio>
#include "../ds/ring_buffer.hh"
#include "../../common/lock/lock.hh"

template <class EventType> class BasicEventQueueT {
protected:
	struct {
		size_t size;
		bool block;
	} config;
	bool isRunning;
	RingBuffer<EventType> *queue;

public:
	BasicEventQueueT( size_t size, bool block = true ) {
		this->config.size = size;
		this->config.block = block;
		this->isRunning = false;
		this->queue = new RingBuffer<EventType>( size, block );
	}

	~BasicEventQueueT() {
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

	inline int count( size_t *size = 0 ) {
		if ( size ) *size = this->config.size;
		return this->queue->GetCount();
	}
};

template<class MixedEventType> class EventQueue {
public:
	BasicEventQueueT<MixedEventType> *mixed;
	struct {
		 // High priority
		BasicEventQueueT<MixedEventType> *mixed;
		LOCK_T lock;
		uint32_t capacity;
		uint32_t count;
	} priority;

	EventQueue() {
		this->mixed = 0;
		this->priority.mixed = 0;
		this->priority.capacity = 0;
		this->priority.count = 0;
	}

	void init( bool block, uint32_t mixed, uint32_t pMixed = 0 ) {
		this->mixed = new BasicEventQueueT<MixedEventType>( mixed, block );
		if ( pMixed )
			this->priority.mixed = new BasicEventQueueT<MixedEventType>( pMixed, false );
		this->priority.capacity = pMixed;
		LOCK_INIT( &this->priority.lock );
	}

	void start() {
		this->mixed->start();
		if ( this->priority.mixed )
			this->priority.mixed->start();
	}

	void stop() {
		this->mixed->stop();
		if ( this->priority.mixed )
			this->priority.mixed->stop();
	}

	void free() {
		delete this->mixed;
		delete this->priority.mixed;
	}

	void print( FILE *f = stdout ) {
		fprintf( f, "[Mixed] " );
		this->mixed->print( f );
		if ( this->priority.mixed ) {
			fprintf( f, "[Mixed (Prioritized)] " );
			this->priority.mixed->print( f );
		}
	}

	bool extractMixed( MixedEventType &event ) {
		if ( this->priority.mixed && this->priority.mixed->extract( event ) ) {
			LOCK( &this->priority.lock );
			this->priority.count--;
			UNLOCK( &this->priority.lock );
			return true;
		} else {
			return this->mixed->extract( event );
		}
	}
};

#define DEFINE_EVENT_QUEUE_INSERT(_EVENT_TYPE_) \
	bool insert( _EVENT_TYPE_ &event ) { \
		MixedEvent mixedEvent; \
		mixedEvent.set( event ); \
		bool ret = this->mixed->insert( mixedEvent ); \
		return ret; \
	}

#define DEFINE_EVENT_QUEUE_PRIORITIZED_INSERT(_EVENT_TYPE_) \
	bool prioritizedInsert( _EVENT_TYPE_ &event ) { \
		bool ret; \
		MixedEvent mixedEvent; \
		mixedEvent.set( event ); \
		size_t count, size; \
		count = ( size_t ) this->mixed->count( &size ); \
		if ( count && TRY_LOCK( &this->priority.lock ) == 0 ) { \
			/* Locked */ \
			if ( this->priority.count < this->priority.capacity ) { \
				this->priority.count++; \
				ret = this->priority.mixed->insert( mixedEvent ); \
				UNLOCK( &this->priority.lock ); \
				/* Avoid all worker threads are blocked by the empty normal queue */ \
				if ( this->mixed->count() < ( int ) count ) { \
					mixedEvent.set(); \
					this->mixed->insert( mixedEvent ); \
				} \
				return ret; \
			} else { \
				UNLOCK( &this->priority.lock ); \
				return this->mixed->insert( mixedEvent ); \
			} \
		} else { \
			ret = this->mixed->insert( mixedEvent ); \
			return ret; \
		} \
	}

#endif

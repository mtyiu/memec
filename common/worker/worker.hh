#ifndef __COMMON_WORKER_WORKER_HH__
#define __COMMON_WORKER_WORKER_HH__

#include <cstdio>
#include <pthread.h>

#define WORKER_RECEIVE_FROM_EVENT_SOCKET() \
	ret = event.socket->recv( \
		this->protocol.buffer.recv, \
		this->protocol.buffer.size, \
		connected, \
		false \
	); \
	buffer.data = this->protocol.buffer.recv; \
	buffer.size = ret > 0 ? ( size_t ) ret : 0

#define WORKER_RECEIVE_WHOLE_MESSAGE_FROM_EVENT_SOCKET(worker_name) \
	if ( buffer.size < PROTO_HEADER_SIZE ) { \
		ret = event.socket->recvRem( \
			this->protocol.buffer.recv, \
			PROTO_HEADER_SIZE, \
			buffer.data, \
			buffer.size, \
			connected \
		); \
		buffer.data = this->protocol.buffer.recv; \
		buffer.size = ret > 0 ? ( size_t ) ret : 0; \
	} \
	if ( ! connected || ! buffer.size ) break; \
	if ( ! this->protocol.parseHeader( header, buffer.data, buffer.size ) ) { \
		__ERROR__( worker_name, "dispatch", "Undefined message (remaining bytes = %lu).", buffer.size ); \
		break; \
	} \
	if ( buffer.size < PROTO_HEADER_SIZE + header.length ) { \
		ret = event.socket->recvRem( \
			this->protocol.buffer.recv, \
			PROTO_HEADER_SIZE + header.length, \
			buffer.data, \
			buffer.size, \
			connected \
		); \
		buffer.data = this->protocol.buffer.recv; \
		buffer.size = ret > 0 ? ( size_t ) ret : 0; \
	} \
	if ( ! connected ) break;

class Worker {
protected:
	bool isRunning;
	pthread_t tid;

	virtual void free() = 0;

public:
	Worker() {
		this->isRunning = false;
		this->tid = 0;
	}

	inline void join() {
		pthread_join( this->tid, 0 );
	}

	inline bool getIsRunning() {
		return this->isRunning;
	}

	inline pthread_t getThread() {
		return this->tid;
	}

	virtual bool start() = 0;
	virtual void stop() = 0;
	virtual void print( FILE *f = stdout ) = 0;
};

#endif

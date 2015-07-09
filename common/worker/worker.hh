#ifndef __COMMON_WORKER_WORKER_HH__
#define __COMMON_WORKER_WORKER_HH__

#include <cstdio>
#include <pthread.h>

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
		pthread_join( this->tid, NULL );
	}

	inline bool getIsRunning() {
		return this->isRunning;
	}

	inline pthread_t getThread() {
		return this->tid;
	}

	virtual bool start() = 0;
	virtual void stop() = 0;
	virtual void debug() = 0;
};

#endif

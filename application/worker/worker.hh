#ifndef __APPLICATION_WORKER_WORKER_HH__
#define __APPLICATION_WORKER_WORKER_HH__

#include <cstdio>
#include "worker_role.hh"
#include "../config/application_config.hh"
#include "../event/event_queue.hh"
#include "../protocol/protocol.hh"
#include "../../common/worker/worker.hh"

class ApplicationWorker : public Worker {
private:
	WorkerRole role;
	ApplicationEventQueue *eventQueue;
	ApplicationProtocol protocol;

	void dispatch( MixedEvent event );
	void dispatch( ApplicationEvent event );
	void dispatch( MasterEvent event );
	void free();
	static void *run( void *argv );

public:
	bool init( ApplicationConfig &config, WorkerRole role, ApplicationEventQueue *eventQueue );
	bool start();
	void stop();
	void print( FILE *f = stdout );

	inline WorkerRole getRole() {
		return this->role;
	}

	inline ApplicationEventQueue *getEventQueue() {
		return this->eventQueue;
	}
};

#endif

#ifndef __COORDINATOR_WORKER_WORKER_HH__
#define __COORDINATOR_WORKER_WORKER_HH__

#include <cstdio>
#include "worker_role.hh"
#include "../event/event_queue.hh"
#include "../protocol/protocol.hh"
#include "../../common/worker/worker.hh"
#include "../../common/config/global_config.hh"

class CoordinatorWorker : public Worker {
private:
	WorkerRole role;
	CoordinatorEventQueue *eventQueue;
	CoordinatorProtocol protocol;

	void dispatch( MixedEvent event );
	void dispatch( CoordinatorEvent event );
	void dispatch( MasterEvent event );
	void dispatch( SlaveEvent event );
	void free();
	static void *run( void *argv );

public:
	bool init( GlobalConfig &config, WorkerRole role, CoordinatorEventQueue *eventQueue );
	bool start();
	void stop();
	void print( FILE *f = stdout );

	inline WorkerRole getRole() {
		return this->role;
	}

	inline CoordinatorEventQueue *getEventQueue() {
		return this->eventQueue;
	}
};

#endif

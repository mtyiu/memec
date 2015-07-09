#ifndef __COORDINATOR_WORKER_WORKER_HH__
#define __COORDINATOR_WORKER_WORKER_HH__

#include "worker_role.hh"
#include "../event/event_queue.hh"
#include "../../common/worker/worker.hh"

class CoordinatorWorker : public Worker {
private:
	WorkerRole role;
	CoordinatorEventQueue *eventQueue;

	void dispatch( MixedEvent event );
	void dispatch( ApplicationEvent event );
	void dispatch( CoordinatorEvent event );
	void dispatch( MasterEvent event );
	void dispatch( SlaveEvent event );
	void free();
	static void *run( void *argv );

public:
	bool init( WorkerRole role, CoordinatorEventQueue *eventQueue );
	bool start();
	void stop();
	void debug();

	inline WorkerRole getRole() {
		return this->role;
	}

	inline CoordinatorEventQueue *getEventQueue() {
		return this->eventQueue;
	}
};

#endif

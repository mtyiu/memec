#ifndef __SLAVE_WORKER_WORKER_HH__
#define __SLAVE_WORKER_WORKER_HH__

#include "worker_role.hh"
#include "../event/event_queue.hh"
#include "../../common/worker/worker.hh"

class SlaveWorker : public Worker {
private:
	WorkerRole role;
	SlaveEventQueue *eventQueue;

	void dispatch( MixedEvent event );
	void dispatch( ApplicationEvent event );
	void dispatch( CoordinatorEvent event );
	void dispatch( MasterEvent event );
	void dispatch( SlaveEvent event );
	void free();
	static void *run( void *argv );

public:
	bool init( WorkerRole role, SlaveEventQueue *eventQueue );
	bool start();
	void stop();
	void debug();

	inline WorkerRole getRole() {
		return this->role;
	}

	inline SlaveEventQueue *getEventQueue() {
		return this->eventQueue;
	}
};

#endif

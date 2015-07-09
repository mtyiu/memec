#ifndef __COORDINATOR_WORKER_WORKER_HH__
#define __COORDINATOR_WORKER_WORKER_HH__

#include "worker_role.hh"
#include "../event/event_queue.hh"
#include "../../common/worker/worker.hh"

class CoordinatorWorker : public Worker {
private:
	WorkerRole role;
	CoordinatorEventQueue *eventQueue;

	void free();
	static void *run( void *args );

public:
	CoordinatorWorker();
	bool init();
	bool start();
	void stop();
	void debug( FILE *f = stdout );
};

#endif

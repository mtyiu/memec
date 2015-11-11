#ifndef __COORDINATOR_REMAP_REMAP_WORKER_HH__
#define __COORDINATOR_REMAP_REMAP_WORKER_HH__

#include "../event/remap_status_event.hh"
#include "../../common/ds/sockaddr_in.hh"
#include "../../common/lock/lock.hh"
#include "../../common/worker/worker.hh"

class CoordinatorRemapWorker: public Worker {
private:
	// individual slaves
	bool startRemap( RemapStatusEvent );
	bool stopRemap( RemapStatusEvent );

public:
	CoordinatorRemapWorker();
	~CoordinatorRemapWorker();

	// main function
	static void *run( void *argv );

	void free();
	bool start();
	void stop();
	void print( FILE *f = stdout );
};

#endif

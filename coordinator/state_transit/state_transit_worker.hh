#ifndef __COORDINATOR_STATE_TRANSIT_STATE_TRANSIT_WORKER_HH__
#define __COORDINATOR_STATE_TRANSIT_STATE_TRANSIT_WORKER_HH__

#include "../event/state_transit_event.hh"
#include "../../common/ds/sockaddr_in.hh"
#include "../../common/lock/lock.hh"
#include "../../common/worker/worker.hh"

class CoordinatorStateTransitWorker: public Worker {
private:
	// individual servers
	bool transitToDegraded( StateTransitEvent );
	bool transitToNormal( StateTransitEvent );

public:
	CoordinatorStateTransitWorker();
	~CoordinatorStateTransitWorker();

	// main function
	static void *run( void *argv );

	void free();
	bool start();
	void stop();
	void print( FILE *f = stdout );
};

#endif

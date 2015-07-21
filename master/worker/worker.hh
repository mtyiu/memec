	#ifndef __MASTER_WORKER_WORKER_HH__
#define __MASTER_WORKER_WORKER_HH__

#include <cstdio>
#include "worker_role.hh"
#include "../event/event_queue.hh"
#include "../protocol/protocol.hh"
#include "../socket/slave_socket.hh"
#include "../../common/worker/worker.hh"
#include "../../common/config/global_config.hh"
#include "../../common/stripe_list/stripe_list.hh"

class MasterWorker : public Worker {
private:
	WorkerRole role;
	MasterProtocol protocol;
	static MasterEventQueue *eventQueue;
	static StripeList<SlaveSocket> *stripeList;

	void dispatch( MixedEvent event );
	void dispatch( ApplicationEvent event );
	void dispatch( CoordinatorEvent event );
	void dispatch( MasterEvent event );
	void dispatch( SlaveEvent event );
	void free();
	static void *run( void *argv );

public:
	static bool init( MasterEventQueue *eventQueue, StripeList<SlaveSocket> *stripeList );
	bool init( GlobalConfig &config, WorkerRole role );
	bool start();
	void stop();
	void print( FILE *f = stdout );

	inline WorkerRole getRole() {
		return this->role;
	}

	inline MasterEventQueue *getEventQueue() {
		return this->eventQueue;
	}
};

#endif

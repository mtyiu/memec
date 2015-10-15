#ifndef __COORDINATOR_WORKER_WORKER_HH__
#define __COORDINATOR_WORKER_WORKER_HH__

#include <cstdio>
#include "worker_role.hh"
#include "../event/event_queue.hh"
#include "../protocol/protocol.hh"
#include "../../common/worker/worker.hh"
#include "../../common/config/global_config.hh"
#include "../../common/ds/id_generator.hh"

class CoordinatorWorker : public Worker {
private:
	uint32_t workerId;
	WorkerRole role;
	CoordinatorProtocol protocol;
	static IDGenerator *idGenerator;
	static CoordinatorEventQueue *eventQueue;

	void dispatch( MixedEvent event );
	void dispatch( CoordinatorEvent event );
	void dispatch( MasterEvent event );
	void dispatch( SlaveEvent event );
	void free();
	static void *run( void *argv );

public:
	bool processHeartbeat( SlaveEvent event, char *buf, size_t size );

	static bool init();
	bool init( GlobalConfig &config, WorkerRole role, uint32_t workerId );
	bool start();
	void stop();
	void print( FILE *f = stdout );

	inline WorkerRole getRole() {
		return this->role;
	}
};

#endif

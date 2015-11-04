#ifndef __COORDINATOR_WORKER_WORKER_HH__
#define __COORDINATOR_WORKER_WORKER_HH__

#include <cstdio>
#include "worker_role.hh"
#include "../../common/ds/remapping_record_map.hh"
#include "../event/event_queue.hh"
#include "../protocol/protocol.hh"
#include "../../common/worker/worker.hh"
#include "../../common/config/global_config.hh"
#include "../../common/ds/id_generator.hh"
#include "../../common/stripe_list/stripe_list.hh"

class CoordinatorWorker : public Worker {
private:
	uint32_t workerId;
	WorkerRole role;
	CoordinatorProtocol protocol;
	static uint32_t dataChunkCount;
	static uint32_t parityChunkCount;
	static uint32_t chunkCount;
	static IDGenerator *idGenerator;
	static CoordinatorEventQueue *eventQueue;
	static StripeList<ServerAddr> *stripeList;

	void dispatch( MixedEvent event );
	void dispatch( CoordinatorEvent event );
	void dispatch( MasterEvent event );
	void dispatch( SlaveEvent event );
	void free();
	static void *run( void *argv );

public:
	static RemappingRecordMap *remappingRecords;
	bool processHeartbeat( SlaveEvent event, char *buf, size_t size );
	bool triggerRecovery( SlaveSocket *socket );

	bool handleDegradedLockRequest( MasterEvent event, char *buf, size_t size );

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

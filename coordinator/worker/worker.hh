#ifndef __COORDINATOR_WORKER_WORKER_HH__
#define __COORDINATOR_WORKER_WORKER_HH__

#include <cstdio>
#include "worker_role.hh"
#include "../ds/pending.hh"
#include "../ds/remapping_record_map.hh"
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
	uint32_t *survivingChunkIds;
	static uint32_t dataChunkCount;
	static uint32_t parityChunkCount;
	static uint32_t chunkCount;
	static IDGenerator *idGenerator;
	static CoordinatorEventQueue *eventQueue;
	static StripeList<SlaveSocket> *stripeList;
	static Pending *pending;

	// ---------- worker.cc ----------
	void dispatch( MixedEvent event );
	void dispatch( CoordinatorEvent event );
	void free();
	static void *run( void *argv );

	// ---------- client_worker.cc ----------
	void dispatch( MasterEvent event );
	bool handleSyncMetadata( MasterEvent event, char *buf, size_t size );

	// ---------- server_worker.cc ----------
	void dispatch( SlaveEvent event );
	bool processHeartbeat( SlaveEvent event, char *buf, size_t size );

	// ---------- remap_worker.cc ----------
	bool handleRemappingSetLockRequest( MasterEvent event, char* buf, size_t size );

	// ---------- degraded_worker.cc ----------
	bool handleDegradedLockRequest( MasterEvent event, char *buf, size_t size );
	bool handleReleaseDegradedLockRequest( SlaveSocket *socket, pthread_mutex_t *lock, pthread_cond_t *cond, bool *done );
	bool handleReleaseDegradedLockResponse( SlaveEvent event, char *buf, size_t size );

	// ---------- recovery_worker.cc ----------
	bool handlePromoteBackupSlaveResponse( SlaveEvent event, char *buf, size_t size );
	bool handleReconstructionRequest( SlaveSocket *socket );
	bool handleReconstructionResponse( SlaveEvent event, char *buf, size_t size );
	bool handleReconstructionUnsealedResponse( SlaveEvent event, char *buf, size_t size );

public:
	static RemappingRecordMap *remappingRecords;

	// ---------- worker.cc ----------
	static bool init();
	bool init( GlobalConfig &config, WorkerRole role, uint32_t workerId );
	bool start();
	void stop();
	void print( FILE *f = stdout );
	inline WorkerRole getRole() { return this->role; }
};

#endif

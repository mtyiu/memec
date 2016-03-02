#ifndef __COORDINATOR_WORKER_WORKER_HH__
#define __COORDINATOR_WORKER_WORKER_HH__

#include <cstdio>
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
	CoordinatorProtocol protocol;
	uint32_t *survivingChunkIds;
	static uint32_t dataChunkCount;
	static uint32_t parityChunkCount;
	static uint32_t chunkCount;
	static IDGenerator *idGenerator;
	static CoordinatorEventQueue *eventQueue;
	static StripeList<ServerSocket> *stripeList;
	static Pending *pending;

	// ---------- worker.cc ----------
	void dispatch( MixedEvent event );
	void dispatch( CoordinatorEvent event );
	void free();
	static void *run( void *argv );

	// ---------- client_worker.cc ----------
	void dispatch( ClientEvent event );
	bool handleSyncMetadata( ClientEvent event, char *buf, size_t size );

	// ---------- server_worker.cc ----------
	void dispatch( ServerEvent event );
	bool processHeartbeat( ServerEvent event, char *buf, size_t size );

	// ---------- remap_worker.cc ----------
	bool handleRemappingSetLockRequest( ClientEvent event, char* buf, size_t size );

	// ---------- degraded_worker.cc ----------
	bool handleDegradedLockRequest( ClientEvent event, char *buf, size_t size );
	bool handleReleaseDegradedLockRequest( ServerSocket *socket, pthread_mutex_t *lock, pthread_cond_t *cond, bool *done );
	bool handleReleaseDegradedLockResponse( ServerEvent event, char *buf, size_t size );

	// ---------- recovery_worker.cc ----------
	bool handlePromoteBackupSlaveResponse( ServerEvent event, char *buf, size_t size );
	bool handleReconstructionRequest( ServerSocket *socket );
	bool handleReconstructionResponse( ServerEvent event, char *buf, size_t size );
	bool handleReconstructionUnsealedResponse( ServerEvent event, char *buf, size_t size );

public:
	static RemappingRecordMap *remappingRecords;

	// ---------- worker.cc ----------
	static bool init();
	bool init( GlobalConfig &config, uint32_t workerId );
	bool start();
	void stop();
	void print( FILE *f = stdout );
};

#endif

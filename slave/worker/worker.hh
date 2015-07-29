#ifndef __SLAVE_WORKER_WORKER_HH__
#define __SLAVE_WORKER_WORKER_HH__

#include <vector>
#include <cstdio>
#include "worker_role.hh"
#include "../buffer/mixed_chunk_buffer.hh"
#include "../event/event_queue.hh"
#include "../protocol/protocol.hh"
#include "../../common/config/global_config.hh"
#include "../../common/map/map.hh"
#include "../../common/stripe_list/stripe_list.hh"
#include "../../common/worker/worker.hh"

class SlaveWorker : public Worker {
private:
	WorkerRole role;
	SlaveProtocol protocol;
	static SlaveEventQueue *eventQueue;
	static StripeList<SlavePeerSocket> *stripeList;
	static Map *map;
	static std::vector<MixedChunkBuffer *> *chunkBuffer;

	void dispatch( MixedEvent event );
	void dispatch( CoordinatorEvent event );
	void dispatch( MasterEvent event );
	void dispatch( SlaveEvent event );
	void dispatch( SlavePeerEvent event );
	void free();
	static void *run( void *argv );

public:
	static bool init( SlaveEventQueue *eventQueue, StripeList<SlavePeerSocket> *stripeList, Map *map, std::vector<MixedChunkBuffer *> *chunkBuffer );
	bool init( GlobalConfig &config, WorkerRole role );
	bool start();
	void stop();
	void print( FILE *f = stdout );

	inline WorkerRole getRole() {
		return this->role;
	}

	inline SlaveEventQueue *getEventQueue() {
		return this->eventQueue;
	}
};

#endif

#ifndef __SLAVE_WORKER_WORKER_HH__
#define __SLAVE_WORKER_WORKER_HH__

#include <vector>
#include <cstdio>
#include "worker_role.hh"
#include "../buffer/mixed_chunk_buffer.hh"
#include "../config/slave_config.hh"
#include "../event/event_queue.hh"
#include "../ds/map.hh"
#include "../ds/pending.hh"
#include "../ds/slave_load.hh"
#include "../protocol/protocol.hh"
#include "../storage/allstorage.hh"
#include "../../common/coding/coding.hh"
#include "../../common/config/global_config.hh"
#include "../../common/ds/chunk.hh"
#include "../../common/ds/key.hh"
#include "../../common/ds/memory_pool.hh"
#include "../../common/ds/stripe.hh"
#include "../../common/stripe_list/stripe_list.hh"
#include "../../common/worker/worker.hh"

#define SLAVE_WORKER_SEND_REPLICAS_PARALLEL

class SlaveWorker : public Worker {
private:
	WorkerRole role;
	SlaveProtocol protocol;
	Storage *storage;
	// Temporary variables
	Chunk *dataChunk, *parityChunk;
	SlavePeerSocket **dataSlaveSockets;
	SlavePeerSocket **paritySlaveSockets;
	static uint32_t dataChunkCount;
	static uint32_t parityChunkCount;
	static Pending *pending;
	static ServerAddr *slaveServerAddr;
	static Coding *coding;
	static SlaveEventQueue *eventQueue;
	static StripeList<SlavePeerSocket> *stripeList;
	static Map *map;
	static MemoryPool<Chunk> *chunkPool;
	static MemoryPool<Stripe> *stripePool;
	static std::vector<MixedChunkBuffer *> *chunkBuffer;

	void dispatch( MixedEvent event );
	void dispatch( CodingEvent event );
	void dispatch( CoordinatorEvent event );
	void dispatch( IOEvent event );
	void dispatch( MasterEvent event );
	void dispatch( SlaveEvent event );
	void dispatch( SlavePeerEvent event );

	bool isRedirectedRequest( char *key, uint8_t size, bool *isParity = 0, uint32_t *listIdPtr = 0, uint32_t *chunkIdPtr = 0 );
	bool isRedirectedRequest( uint32_t listId );

	SlavePeerSocket *getSlave( char *data, uint8_t size, uint32_t &listId, uint32_t &chunkId, bool allowDegraded = false, bool *isDegraded = 0 );
	SlavePeerSocket *getSlaves( char *data, uint8_t size, uint32_t &listId, uint32_t &chunkId, bool allowDegraded = false, bool *isDegraded = 0 );

	bool handleGetRequest( MasterEvent event );
	bool handleSetRequest( MasterEvent event );
	bool handleUpdateRequest( MasterEvent event );
	bool handleDeleteRequest( MasterEvent event );

	bool handleSlavePeerRegisterRequest( SlavePeerSocket *socket, ProtocolHeader &header, ssize_t recvBytes );
	bool handleUpdateChunkRequest( SlavePeerEvent event );
	bool handleDeleteChunkRequest( SlavePeerEvent event );
	bool handleUpdateChunkResponse( SlavePeerEvent event, bool success );
	bool handleDeleteChunkResponse( SlavePeerEvent event, bool success );

	void free();
	static void *run( void *argv );

public:
	SlaveLoad load;

	static bool init();
	bool init( GlobalConfig &globalConfig, SlaveConfig &slaveConfig, WorkerRole role );
	bool start();
	void stop();
	void print( FILE *f = stdout );

	inline WorkerRole getRole() {
		return this->role;
	}
};

#endif

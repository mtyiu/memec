#ifndef __MASTER_WORKER_WORKER_HH__
#define __MASTER_WORKER_WORKER_HH__

#include <cstdio>
#include "worker_role.hh"
#include "../ds/counter.hh"
#include "../ds/pending.hh"
#include "../ds/remap_flag.hh"
#include "../event/event_queue.hh"
#include "../protocol/protocol.hh"
#include "../socket/slave_socket.hh"
#include "../../common/worker/worker.hh"
#include "../../common/config/global_config.hh"
#include "../../common/stripe_list/stripe_list.hh"
#include "../../common/ds/id_generator.hh"
#include "../../common/ds/packet_pool.hh"
#include "../../common/ds/sockaddr_in.hh"

#define MASTER_WORKER_SEND_REPLICAS_PARALLEL

class MasterWorker : public Worker {
private:
	uint32_t workerId;
	WorkerRole role;
	MasterProtocol protocol;
	// Temporary variables
	SlaveSocket **dataSlaveSockets;
	SlaveSocket **paritySlaveSockets;
	static uint32_t dataChunkCount;
	static uint32_t parityChunkCount;
	static IDGenerator *idGenerator;
	static Pending *pending;
	static MasterEventQueue *eventQueue;
	static StripeList<SlaveSocket> *stripeList;
	static Counter *counter;
	static RemapFlag *remapFlag;
	static PacketPool *packetPool;

	void dispatch( MixedEvent event );
	void dispatch( ApplicationEvent event );
	void dispatch( CoordinatorEvent event );
	void dispatch( MasterEvent event );
	void dispatch( SlaveEvent event );

	SlaveSocket *getSlave( char *data, uint8_t size, uint32_t &listId, uint32_t &chunkId, bool allowDegraded = false, bool *isDegraded = 0 );
	SlaveSocket *getSlave( char *data, uint8_t size, uint32_t &originalListId, uint32_t &originalChunkId, uint32_t &remappedListId, uint32_t &remappedChunkId, bool allowDegraded = false, bool *isDegraded = 0 ); // for remapping
	SlaveSocket *getSlaves( char *data, uint8_t size, uint32_t &listId, uint32_t &chunkId, bool allowDegraded = false, bool *isDegraded = 0 );
	SlaveSocket *getSlaves( uint32_t listId, uint32_t chunkId, bool allowDegraded, bool *isDegraded );

	bool handleGetRequest( ApplicationEvent event, char *buf, size_t size );
	bool handleSetRequest( ApplicationEvent event, char *buf, size_t size );
	bool handleRemappingSetRequest( ApplicationEvent event, char *buf, size_t size );
	bool handleUpdateRequest( ApplicationEvent event, char *buf, size_t size );
	bool handleDeleteRequest( ApplicationEvent event, char *buf, size_t size );

	bool handleGetResponse( SlaveEvent event, bool success, char *buf, size_t size );
	bool handleSetResponse( SlaveEvent event, bool success, char *buf, size_t size );
	bool handleRemappingSetLockResponse( SlaveEvent event, bool success, char *buf, size_t size );
	bool handleRemappingSetResponse( SlaveEvent event, bool success, char *buf, size_t size );
	bool handleUpdateResponse( SlaveEvent event, bool success, char *buf, size_t size );
	bool handleDeleteResponse( SlaveEvent event, bool success, char *buf, size_t size );

	void free();
	static void *run( void *argv );

public:
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

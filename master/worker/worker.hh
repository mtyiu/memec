#ifndef __MASTER_WORKER_WORKER_HH__
#define __MASTER_WORKER_WORKER_HH__

#include <cstdio>
#include "worker_role.hh"
#include "../ds/counter.hh"
#include "../ds/pending.hh"
#include "../event/event_queue.hh"
#include "../protocol/protocol.hh"
#include "../remap/remap_msg_handler.hh"
#include "../socket/slave_socket.hh"
#include "../../common/worker/worker.hh"
#include "../../common/config/global_config.hh"
#include "../../common/stripe_list/stripe_list.hh"
#include "../../common/ds/id_generator.hh"
#include "../../common/ds/packet_pool.hh"
#include "../../common/ds/sockaddr_in.hh"
#include "../../common/timestamp/timestamp.hh"

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
	static uint32_t updateInterval;
	static bool disableRemappingSet;
	static bool degradedTargetIsFixed;
	static IDGenerator *idGenerator;
	static Pending *pending;
	static MasterEventQueue *eventQueue;
	static StripeList<SlaveSocket> *stripeList;
	//static Counter *counter;
	static ArrayMap<int, SlaveSocket> *slaveSockets;
	static PacketPool *packetPool;
	static MasterRemapMsgHandler *remapMsgHandler;
	static Timestamp *timestamp;

	// ---------- worker.cc ----------
	void dispatch( MixedEvent event );
	void dispatch( MasterEvent event );
	// For normal operations
	SlaveSocket *getSlaves( char *data, uint8_t size, uint32_t &listId, uint32_t &chunkId );
	// For degraded GET
	SlaveSocket *getSlaves(
		char *data, uint8_t size, uint32_t &listId, uint32_t &chunkId, uint32_t &newChunkId,
		bool &useDegradedMode, SlaveSocket *&original
	);
	// For degraded UPDATE / DELETE (which may involve failed parity slaves)
	// Return the data server for handling the request
	SlaveSocket *getSlaves(
		char *data, uint8_t size, uint32_t &listId,
		uint32_t &dataChunkId, uint32_t &newDataChunkId,
		uint32_t &parityChunkId, uint32_t &newParityChunkId,
		bool &useDegradedMode
	);
	// For remapping
	SlaveSocket *getSlaves(
		char *data, uint8_t size,
		uint32_t &originalListId, uint32_t &originalChunkId,
		uint32_t &remappedListId, std::vector<uint32_t> &remappedChunkId
	);
	SlaveSocket *getSlaves( uint32_t listId, uint32_t chunkId );
	void free();
	static void *run( void *argv );

	// ---------- coordinator_worker.cc ----------
	void dispatch( CoordinatorEvent event );

	// ---------- application_worker.cc ----------
	void dispatch( ApplicationEvent event );
	bool handleSetRequest( ApplicationEvent event, char *buf, size_t size );
	bool handleGetRequest( ApplicationEvent event, char *buf, size_t size );
	bool handleUpdateRequest( ApplicationEvent event, char *buf, size_t size );
	bool handleDeleteRequest( ApplicationEvent event, char *buf, size_t size );

	// ---------- slave_worker.cc ----------
	void dispatch( SlaveEvent event );
	bool handleSetResponse( SlaveEvent event, bool success, char *buf, size_t size );
	bool handleGetResponse( SlaveEvent event, bool success, bool isDegraded, char *buf, size_t size );
	bool handleUpdateResponse( SlaveEvent event, bool success, bool isDegraded, char *buf, size_t size );
	bool handleDeleteResponse( SlaveEvent event, bool success, bool isDegraded, char *buf, size_t size );
	bool handleAcknowledgement( SlaveEvent event, uint8_t opcode, char *buf, size_t size );

	// ---------- degraded_worker.cc ----------
	bool sendDegradedLockRequest(
		uint16_t parentInstanceId, uint32_t parentRequestId, uint8_t opcode,
		uint32_t listId,
		uint32_t dataChunkId, uint32_t newDataChunkId,
		uint32_t parityChunkId, uint32_t newParityChunkId,
		char *key, uint8_t keySize,
		uint32_t valueUpdateSize = 0, uint32_t valueUpdateOffset = 0, char *valueUpdate = 0
	);
	bool handleDegradedLockResponse( CoordinatorEvent event, bool success, char *buf, size_t size );

	// ---------- remap_worker.cc ----------
	bool handleRemappingSetRequest( ApplicationEvent event, char *buf, size_t size );
	bool handleRemappingSetLockResponse( CoordinatorEvent event, bool success, char *buf, size_t size );
	bool handleRemappingSetResponse( SlaveEvent event, bool success, char *buf, size_t size );

public:
	// ---------- worker.cc ----------
	static bool init();
	bool init( GlobalConfig &config, WorkerRole role, uint32_t workerId );
	bool start();
	void stop();
	void print( FILE *f = stdout );
	inline WorkerRole getRole() { return this->role; }
};

#endif

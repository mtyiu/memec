#ifndef __MASTER_WORKER_WORKER_HH__
#define __MASTER_WORKER_WORKER_HH__

#include <cstdio>
#include "worker_role.hh"
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

#define MASTER_WORKER_SEND_REPLICAS_PARALLEL

class MasterWorker : public Worker {
private:
	uint32_t workerId;
	WorkerRole role;
	MasterProtocol protocol;
	// Temporary variables
	uint32_t *original, *remapped;
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
	static ArrayMap<int, SlaveSocket> *slaveSockets;
	static PacketPool *packetPool;
	static MasterRemapMsgHandler *remapMsgHandler;

	// ---------- worker.cc ----------
	void dispatch( MixedEvent event );
	void dispatch( MasterEvent event );
	// For normal operations
	SlaveSocket *getSlaves( char *data, uint8_t size, uint32_t &listId, uint32_t &chunkId );
	// For both remapping and degraded operations
	bool getSlaves(
		uint8_t opcode, char *data, uint8_t size,
		uint32_t *&original, uint32_t *&remapped, uint32_t &remappedCount,
		SlaveSocket *&originalDataSlaveSocket, bool &useCoordinatedFlow
	);
	SlaveSocket *getSlaves( uint32_t listId, uint32_t chunkId );

	// For degraded GET
	// SlaveSocket *getSlaves(
	// 	char *data, uint8_t size, uint32_t &listId, uint32_t &chunkId, uint32_t &newChunkId,
	// 	bool &useDegradedMode, SlaveSocket *&original
	// );
	// For degraded UPDATE / DELETE (which may involve failed parity slaves)
	// Return the data server for handling the request
	// SlaveSocket *getSlaves(
	// 	char *data, uint8_t size, uint32_t &listId,
	// 	uint32_t &dataChunkId, uint32_t &newDataChunkId,
	// 	uint32_t &parityChunkId, uint32_t &newParityChunkId,
	// 	bool &useDegradedMode
	// );
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
	bool handleParityDeltaAcknowledgement( SlaveEvent event, uint8_t opcode, char *buf, size_t size );

	// ---------- degraded_worker.cc ----------
	bool sendDegradedLockRequest(
		uint16_t parentInstanceId, uint32_t parentRequestId, uint8_t opcode,
		uint32_t *original, uint32_t *reconstructed, uint32_t reconstructedCount,
		char *key, uint8_t keySize,
		uint32_t valueUpdateSize = 0, uint32_t valueUpdateOffset = 0, char *valueUpdate = 0
	);
	bool handleDegradedLockResponse( CoordinatorEvent event, bool success, char *buf, size_t size );

	// ---------- remap_worker.cc ----------
	bool handleRemappingSetRequest( ApplicationEvent event, char *buf, size_t size );
	bool handleRemappingSetLockResponse( CoordinatorEvent event, bool success, char *buf, size_t size );
	bool handleRemappingSetResponse( SlaveEvent event, bool success, char *buf, size_t size );

	// ---------- recovery_worker.cc ----------
	bool handleSlaveReconstructedMsg( CoordinatorEvent event, char *buf, size_t size );

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

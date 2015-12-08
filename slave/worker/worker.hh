#ifndef __SLAVE_WORKER_WORKER_HH__
#define __SLAVE_WORKER_WORKER_HH__

#include <vector>
#include <cstdio>
#include "worker_role.hh"
#include "../buffer/mixed_chunk_buffer.hh"
#include "../buffer/degraded_chunk_buffer.hh"
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
#include "../../common/ds/id_generator.hh"
#include "../../common/ds/key.hh"
#include "../../common/ds/memory_pool.hh"
#include "../../common/ds/packet_pool.hh"
#include "../../common/stripe_list/stripe_list.hh"
#include "../../common/worker/worker.hh"

#define SLAVE_WORKER_SEND_REPLICAS_PARALLEL

class SlaveWorker : public Worker {
private:
	uint32_t workerId;
	WorkerRole role;
	SlaveProtocol protocol;
	Storage *storage;
	// Temporary variables
	struct { // Buffer for storing data delta
		char *data;
		uint32_t size;
	} buffer;
	BitmaskArray *chunkStatus;
	Chunk *dataChunk, *parityChunk;
	Chunk **chunks;
	Chunk *freeChunks;
	SlavePeerSocket **dataSlaveSockets;
	SlavePeerSocket **paritySlaveSockets;

	static uint32_t dataChunkCount;
	static uint32_t parityChunkCount;
	static uint32_t chunkCount;
	static IDGenerator *idGenerator;
	static ArrayMap<int, SlavePeerSocket> *slavePeers;
	static Pending *pending;
	static ServerAddr *slaveServerAddr;
	static Coding *coding;
	static SlaveEventQueue *eventQueue;
	static StripeList<SlavePeerSocket> *stripeList;
	static std::vector<StripeListIndex> *stripeListIndex;
	static Map *map;
	static MemoryPool<Chunk> *chunkPool;
	static std::vector<MixedChunkBuffer *> *chunkBuffer;
	static DegradedChunkBuffer *degradedChunkBuffer;
	static PacketPool *packetPool;

	// ---------- worker.cc ----------
	void dispatch( MixedEvent event );
	void dispatch( CodingEvent event );
	void dispatch( IOEvent event );
	void dispatch( SlaveEvent event );
	SlavePeerSocket *getSlaves( char *data, uint8_t size, uint32_t &listId, uint32_t &chunkId );
	bool getSlaves( uint32_t listId );
	void free();
	static void *run( void *argv );

	// ---------- coordinator_worker.cc ----------
	void dispatch( CoordinatorEvent event );
	bool handleSlaveConnectedMsg( CoordinatorEvent event, char *buf, size_t size );

	// ---------- master_worker.cc ----------
	void dispatch( MasterEvent event );
	bool handleGetRequest( MasterEvent event, char *buf, size_t size );
	bool handleSetRequest( MasterEvent event, char *buf, size_t size, bool needResSet = true );
	bool handleUpdateRequest( MasterEvent event, char *buf, size_t size );
	bool handleDeleteRequest( MasterEvent event, char *buf, size_t size );

	// ---------- slave_peer_worker.cc ----------
	void dispatch( SlavePeerEvent event );

	// ---------- slave_peer_req_worker.cc ----------
	bool handleSlavePeerRegisterRequest( SlavePeerSocket *socket, char *buf, size_t size );
	bool handleSetRequest( SlavePeerEvent event, char *buf, size_t size );
	bool handleGetRequest( SlavePeerEvent event, char *buf, size_t size );
	bool handleUpdateRequest( SlavePeerEvent event, char *buf, size_t size );
	bool handleDeleteRequest( SlavePeerEvent event, char *buf, size_t size );

	bool handleGetChunkRequest( SlavePeerEvent event, char *buf, size_t size );
	bool handleSetChunkRequest( SlavePeerEvent event, bool isSealed, char *buf, size_t size );
	bool handleUpdateChunkRequest( SlavePeerEvent event, char *buf, size_t size );
	bool handleDeleteChunkRequest( SlavePeerEvent event, char *buf, size_t size );
	bool handleSealChunkRequest( SlavePeerEvent event, char *buf, size_t size );

	// ---------- slave_peer_res_worker.cc ----------
	bool handleSetResponse( SlavePeerEvent event, bool success, char *buf, size_t size );
	bool handleGetResponse( SlavePeerEvent event, bool success, char *buf, size_t size );
	bool handleUpdateResponse( SlavePeerEvent event, bool success, char *buf, size_t size );
	bool handleDeleteResponse( SlavePeerEvent event, bool success, char *buf, size_t size );
	bool handleGetChunkResponse( SlavePeerEvent event, bool success, char *buf, size_t size );
	bool handleSetChunkResponse( SlavePeerEvent event, bool success, char *buf, size_t size );
	bool handleUpdateChunkResponse( SlavePeerEvent event, bool success, char *buf, size_t size );
	bool handleDeleteChunkResponse( SlavePeerEvent event, bool success, char *buf, size_t size );
	bool handleSealChunkResponse( SlavePeerEvent event, bool success, char *buf, size_t size );

	// ---------- remap_worker.cc ----------
	bool handleRemappedParity( CoordinatorEvent event, char *buf, size_t size );
	bool handleRemappingSetRequest( MasterEvent event, char *buf, size_t size );
	bool handleRemappingSetRequest( SlavePeerEvent event, char *buf, size_t size );
	bool handleRemappingSetResponse( SlavePeerEvent event, bool success, char *buf, size_t size );

	// ---------- degraded_worker.cc ----------
	bool handleReleaseDegradedLockRequest( CoordinatorEvent event, char *buf, size_t size );
	bool handleDegradedGetRequest( MasterEvent event, char *buf, size_t size );
	bool handleDegradedUpdateRequest( MasterEvent event, char *buf, size_t size );
	bool handleDegradedDeleteRequest( MasterEvent event, char *buf, size_t size );
	bool performDegradedRead(
		MasterSocket *masterSocket,
		uint32_t listId, uint32_t stripeId, uint32_t lostChunkId,
		bool isSealed, uint8_t opcode, uint32_t parentId,
		Key *key, KeyValueUpdate *keyValueUpdate = 0
	);
	bool sendModifyChunkRequest(
		uint32_t parentId, uint8_t keySize, char *keyStr,
		Metadata &metadata, uint32_t offset,
		uint32_t deltaSize, /* valueUpdateSize */
		uint32_t valueUpdateOffset,
		char *delta,        /* valueUpdate */
		bool isSealed, bool isUpdate
	);

	// ---------- recovery_worker.cc ----------
	bool handleSlaveReconstructedMsg( CoordinatorEvent event, char *buf, size_t size );
	bool handleBackupSlavePromotedMsg( CoordinatorEvent event, char *buf, size_t size );
	bool handleReconstructionRequest( CoordinatorEvent event, char *buf, size_t size );

public:
	static unsigned int delay;
	SlaveLoad load;

	// ---------- worker.cc ----------
	static bool init();
	bool init( GlobalConfig &globalConfig, SlaveConfig &slaveConfig, WorkerRole role, uint32_t workerId );
	bool start();
	void stop();
	void print( FILE *f = stdout );
	inline WorkerRole getRole() { return this->role; }

	// ---------- slave_peer_req_worker.cc ----------
	bool issueSealChunkRequest( Chunk *chunk, uint32_t startPos = 0 );
};

#endif

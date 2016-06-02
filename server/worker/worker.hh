#ifndef __SERVER_WORKER_WORKER_HH__
#define __SERVER_WORKER_WORKER_HH__

#include <vector>
#include <cstdio>
#include "../ack/pending_ack.hh"
#include "../buffer/mixed_chunk_buffer.hh"
#include "../buffer/degraded_chunk_buffer.hh"
#include "../buffer/get_chunk_buffer.hh"
#include "../buffer/remapped_buffer.hh"
#include "../config/server_config.hh"
#include "../event/event_queue.hh"
#include "../ds/map.hh"
#include "../ds/pending.hh"
#include "../protocol/protocol.hh"
#include "../storage/allstorage.hh"
#include "../../common/coding/coding.hh"
#include "../../common/config/global_config.hh"
#include "../../common/ds/chunk.hh"
#include "../../common/ds/chunk_pool.hh"
#include "../../common/ds/id_generator.hh"
#include "../../common/ds/key.hh"
#include "../../common/ds/memory_pool.hh"
#include "../../common/ds/packet_pool.hh"
#include "../../common/stripe_list/stripe_list.hh"
#include "../../common/timestamp/timestamp.hh"
#include "../../common/worker/worker.hh"

#define SERVER_WORKER_SEND_REPLICAS_PARALLEL

class ServerWorker : public Worker {
private:
	uint32_t workerId;
	ServerProtocol protocol;
	TempChunkPool tempChunkPool;
	Storage *storage;
	// Temporary variables
	struct { // Buffer for storing data delta
		char *data;
		uint32_t size;
	} buffer;
	BitmaskArray *chunkStatus;
	BitmaskArray *chunkStatusBackup;
	Chunk *dataChunk, *parityChunk;
	Chunk **chunks;
	struct {
		Chunk *dataChunk, *parityChunk;
		Chunk **chunks;
	} forward; // For forwarding parity chunk
	Chunk **freeChunks;
	ServerPeerSocket **dataServerSockets;
	ServerPeerSocket **parityServerSockets;
	bool **sealIndicators;

	static uint32_t dataChunkCount;
	static uint32_t parityChunkCount;
	static uint32_t chunkCount;
	static IDGenerator *idGenerator;
	static ArrayMap<int, ServerPeerSocket> *serverPeers;
	static Pending *pending;
	static ServerEventQueue *eventQueue;
	static StripeList<ServerPeerSocket> *stripeList;
	static Map *map;
	static std::vector<MixedChunkBuffer *> *chunkBuffer;
	static GetChunkBuffer *getChunkBuffer;
	static DegradedChunkBuffer *degradedChunkBuffer;
	static RemappedBuffer *remappedBuffer;
	static PacketPool *packetPool;
	static ChunkPool *chunkPool;

	// ---------- worker.cc ----------
	void dispatch( MixedEvent event );
	void dispatch( CodingEvent event );
	void dispatch( IOEvent event );
	ServerPeerSocket *getServers( char *data, uint8_t size, uint32_t &listId, uint32_t &chunkId );
	bool getServers( uint32_t listId );
	void free();
	static void *run( void *argv );

	// ---------- coordinator_worker.cc ----------
	void dispatch( CoordinatorEvent event );
	bool handleServerConnectedMsg( CoordinatorEvent event, char *buf, size_t size );
	bool handleHeartbeatAck( CoordinatorEvent event, char *buf, size_t size );

	// ---------- client_worker.cc ----------
	void dispatch( ClientEvent event );
	bool handleGetRequest( ClientEvent event, char *buf, size_t size );
	bool handleGetRequest( ClientEvent event, KeyHeader &header, bool isDegraded );
	bool handleSetRequest( ClientEvent event, char *buf, size_t size, bool needResSet = true );
	bool handleSetRequest( ClientEvent event, KeyValueHeader &header, bool needResSet = true );
	bool handleUpdateRequest( ClientEvent event, char *buf, size_t size, bool checkGetChunk );
	bool handleUpdateRequest(
		ClientEvent event, KeyValueUpdateHeader &header,
		uint32_t *original = 0, uint32_t *reconstructed = 0, uint32_t reconstructedCount = 0,
		bool reconstructParity = false,
		Chunk **chunks = 0,
		bool endOfDegradedOp = false,
		bool checkGetChunk = false
	);
	bool handleDeleteRequest( ClientEvent event, char *buf, size_t size, bool checkGetChunk );
	bool handleDeleteRequest(
		ClientEvent event, KeyHeader &header,
		uint32_t *original = 0, uint32_t *reconstructed = 0, uint32_t reconstructedCount = 0,
		bool reconstructParity = false,
		Chunk **chunks = 0,
		bool endOfDegradedOp = false,
		bool checkGetChunk = false
	);

	bool handleAckParityDeltaBackup( ClientEvent event, char *buf, size_t size );
	bool handleRevertDelta( ClientEvent event, char *buf, size_t size );

	// ---------- server_peer_worker.cc ----------
	void dispatch( ServerPeerEvent event );

	// ---------- server_peer_req_worker.cc ----------
	bool handleServerPeerRegisterRequest( ServerPeerSocket *socket, uint16_t instanceId, uint32_t requestId, char *buf, size_t size );
	bool handleForwardKeyRequest( ServerPeerEvent event, char *buf, size_t size );
	bool handleForwardKeyRequest( ServerPeerEvent event, struct ForwardKeyHeader &header, bool self );
	bool handleSetRequest( ServerPeerEvent event, char *buf, size_t size );
	bool handleGetRequest( ServerPeerEvent event, char *buf, size_t size );
	bool handleUpdateRequest( ServerPeerEvent event, char *buf, size_t size );
	bool handleDeleteRequest( ServerPeerEvent event, char *buf, size_t size );

	bool handleGetChunkRequest( ServerPeerEvent event, char *buf, size_t size );
	bool handleGetChunkRequest( ServerPeerEvent event, struct ChunkHeader &header );
	bool handleSetChunkRequest( ServerPeerEvent event, bool isSealed, char *buf, size_t size );
	bool handleUpdateChunkRequest( ServerPeerEvent event, char *buf, size_t size, bool checkGetChunk );
	bool handleDeleteChunkRequest( ServerPeerEvent event, char *buf, size_t size, bool checkGetChunk );
	bool handleSealChunkRequest( ServerPeerEvent event, char *buf, size_t size );
	bool handleBatchKeyValueRequest( ServerPeerEvent event, char *buf, size_t size );
	bool handleBatchChunksRequest( ServerPeerEvent event, char *buf, size_t size );

	// ---------- server_peer_res_worker.cc ----------
	bool handleForwardKeyResponse( ServerPeerEvent event, bool success, char *buf, size_t size );
	bool handleForwardKeyResponse( struct ForwardKeyHeader &header, bool success, bool self );
	bool handleSetResponse( ServerPeerEvent event, bool success, char *buf, size_t size );
	bool handleGetResponse( ServerPeerEvent event, bool success, char *buf, size_t size );
	bool handleUpdateResponse( ServerPeerEvent event, bool success, char *buf, size_t size );
	bool handleDeleteResponse( ServerPeerEvent event, bool success, char *buf, size_t size );
	bool handleGetChunkResponse( ServerPeerEvent event, bool success, char *buf, size_t size );
	bool handleSetChunkResponse( ServerPeerEvent event, bool success, char *buf, size_t size );
	bool handleUpdateChunkResponse( ServerPeerEvent event, bool success, char *buf, size_t size );
	bool handleDeleteChunkResponse( ServerPeerEvent event, bool success, char *buf, size_t size );
	bool handleSealChunkResponse( ServerPeerEvent event, bool success, char *buf, size_t size );
	bool handleBatchKeyValueResponse( ServerPeerEvent event, bool success, char *buf, size_t size );

	// ---------- remap_worker.cc ----------
	bool handleRemappedData( CoordinatorEvent event, char *buf, size_t size );
	bool handleDegradedSetRequest( ClientEvent event, char *buf, size_t size );
	bool handleRemappedUpdateRequest( ServerPeerEvent event, char *buf, size_t size );
	bool handleRemappedDeleteRequest( ServerPeerEvent event, char *buf, size_t size );
	bool handleRemappedUpdateResponse( ServerPeerEvent event, bool success, char *buf, size_t size );
	bool handleRemappedDeleteResponse( ServerPeerEvent event, bool success, char *buf, size_t size );

	// ---------- degraded_worker.cc ----------
	int findInRedirectedList(
		uint32_t *original, uint32_t *reconstructed, uint32_t reconstructedCount,
		uint32_t ongoingAtChunk, bool &reconstructParity, bool &reconstructData,
		uint32_t dataChunkId, bool isSealed
	);
	bool handleReleaseDegradedLockRequest( CoordinatorEvent event, char *buf, size_t size );
	bool handleDegradedGetRequest( ClientEvent event, char *buf, size_t size );
	bool handleDegradedUpdateRequest( ClientEvent event, char *buf, size_t size );
	bool handleDegradedUpdateRequest( ClientEvent event, struct DegradedReqHeader &header, bool jump = false );
	bool handleDegradedDeleteRequest( ClientEvent event, char *buf, size_t size );
	bool handleForwardChunkRequest( ServerPeerEvent event, char *buf, size_t size );
	bool handleForwardChunkRequest( struct ChunkDataHeader &header, bool xorIfExists );
	bool handleForwardChunkResponse( ServerPeerEvent event, bool success, char *buf, size_t size );
	bool performDegradedRead(
		uint8_t opcode,
		ClientSocket *clientSocket,
		uint16_t parentInstanceId, uint32_t parentRequestId,
		uint32_t listId, uint32_t stripeId, uint32_t chunkId, // chunkId refers to the current chunk ID
		Key *key, bool isSealed,
		uint32_t *original, uint32_t *reconstructed, uint32_t reconstructedCount,
		uint32_t ongoingAtChunk, uint8_t numSurvivingChunkIds, uint32_t *survivingChunkIds,
		bool &isReconstructed,
		KeyValueUpdate *keyValueUpdate = 0,
		uint32_t timestamp = 0
	);
	bool sendModifyChunkRequest(
		uint16_t parentInstanceId, uint32_t parentRequestId,
		uint8_t keySize, char *keyStr,
		Metadata &metadata, uint32_t offset,
		uint32_t deltaSize, // valueUpdateSize
		uint32_t valueUpdateOffset,
		char *delta,        // valueUpdate
		bool isSealed, bool isUpdate,
		uint32_t timestamp = 0,
		ClientSocket *clientSocket = 0,
		uint32_t *original = 0, uint32_t *reconstructed = 0, uint32_t reconstructedCount = 0,
		bool reconstructParity = false,
		Chunk **chunks = 0, bool endOfDegradedOp = false,
		bool checkGetChunk = false
	);

	// Perform UPDATE/DELETE on local data chunk and send reconstructed and modified parity chunks to the failed parity servers
	bool handleUpdateRequestBySetChunk( ClientEvent event, KeyValueUpdateHeader &header );

	// ---------- recovery_worker.cc ----------
	bool handleServerReconstructedMsg( CoordinatorEvent event, char *buf, size_t size );
	bool handleBackupServerPromotedMsg( CoordinatorEvent event, char *buf, size_t size );
	bool handleReconstructionRequest( CoordinatorEvent event, char *buf, size_t size );
	bool handleReconstructionUnsealedRequest( CoordinatorEvent event, char *buf, size_t size );
	bool handleCompletedReconstructionAck();

public:
	static unsigned int delay;

	// ---------- worker.cc ----------
	static bool init();
	bool init( GlobalConfig &globalConfig, ServerConfig &serverConfig, uint32_t workerId );
	bool start();
	void stop();
	void print( FILE *f = stdout );

	// ---------- server_peer_req_worker.cc ----------
	bool issueSealChunkRequest( Chunk *chunk, uint32_t startPos = 0 );
};

#endif

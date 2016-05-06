#ifndef __CLIENT_WORKER_WORKER_HH__
#define __CLIENT_WORKER_WORKER_HH__

#include <cstdio>
#include "../ds/pending.hh"
#include "../event/event_queue.hh"
#include "../protocol/protocol.hh"
#include "../state_transit/state_transit_handler.hh"
#include "../socket/server_socket.hh"
#include "../../common/worker/worker.hh"
#include "../../common/config/global_config.hh"
#include "../../common/stripe_list/stripe_list.hh"
#include "../../common/ds/id_generator.hh"
#include "../../common/ds/packet_pool.hh"
#include "../../common/ds/sockaddr_in.hh"

#define CLIENT_WORKER_SEND_REPLICAS_PARALLEL

class ClientWorker : public Worker {
private:
	uint32_t workerId;
	ClientProtocol protocol;
	// Temporary variables
	uint32_t *original, *remapped;
	ServerSocket **dataServerSockets;
	ServerSocket **parityServerSockets;
	static uint32_t dataChunkCount;
	static uint32_t parityChunkCount;
	static uint32_t updateInterval;
	static bool disableDegraded;
	static IDGenerator *idGenerator;
	static ClientEventQueue *eventQueue;
	static StripeList<ServerSocket> *stripeList;
	static ArrayMap<int, ServerSocket> *serverSockets;
	static PacketPool *packetPool;
	static ClientStateTransitHandler *stateTransitHandler;

	// ---------- worker.cc ----------
	void dispatch( MixedEvent event );
	// For normal operations
	ServerSocket *getServers( char *data, uint8_t size, uint32_t &listId, uint32_t &chunkId );
	// For both remapping and degraded operations
	bool getServers(
		uint8_t opcode, char *data, uint8_t size,
		uint32_t *&original, uint32_t *&remapped, uint32_t &remappedCount,
		ServerSocket *&originalDataServerSocket, bool &useCoordinatedFlow
	);
	ServerSocket *getServers( uint32_t listId, uint32_t chunkId );

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

	// ---------- server_worker.cc ----------
	void dispatch( ServerEvent event );
	bool handleSetResponse( ServerEvent event, bool success, char *buf, size_t size );
	bool handleGetResponse( ServerEvent event, bool success, bool isDegraded, char *buf, size_t size );
	bool handleUpdateResponse( ServerEvent event, bool success, bool isDegraded, char *buf, size_t size );
	bool handleDeleteResponse( ServerEvent event, bool success, bool isDegraded, char *buf, size_t size );
	bool handleAcknowledgement( ServerEvent event, uint8_t opcode, char *buf, size_t size );
	bool handleDeltaAcknowledgement( ServerEvent event, uint8_t opcode, char *buf, size_t size );

	// ---------- degraded_worker.cc ----------
	bool sendDegradedLockRequest(
		uint16_t parentInstanceId, uint32_t parentRequestId, uint8_t opcode,
		uint32_t *original, uint32_t *reconstructed, uint32_t reconstructedCount,
		char *key, uint8_t keySize,
		uint32_t valueUpdateSize = 0, uint32_t valueUpdateOffset = 0, char *valueUpdate = 0
	);
	bool handleDegradedLockResponse( CoordinatorEvent event, bool success, char *buf, size_t size );

	// ---------- remap_worker.cc ----------
	bool handleDegradedSetRequest( ApplicationEvent event, char *buf, size_t size );
	bool handleDegradedSetLockResponse( CoordinatorEvent event, bool success, char *buf, size_t size );
	bool handleDegradedSetResponse( ServerEvent event, bool success, char *buf, size_t size );

	// ---------- recovery_worker.cc ----------
	bool handleServerReconstructedMsg( CoordinatorEvent event, char *buf, size_t size );

public:
	static Pending *pending;

	// ---------- worker.cc ----------
	static bool init();
	bool init( GlobalConfig &config, uint32_t workerId );
	bool start();
	void stop();
	void print( FILE *f = stdout );

	static void removePending( ServerSocket *server, bool needsAck = true );
	static void replayRequestPrepare( ServerSocket *server );
	static void replayRequest( ServerSocket *server );
	static void gatherPendingNormalRequests( ServerSocket *target, bool needsAck = false );

	bool handleCoordinatorRequestOnKey( CoordinatorEvent event, char *buf, size_t size, uint8_t opcode );
};

#endif

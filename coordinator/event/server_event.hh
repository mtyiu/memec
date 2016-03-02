#ifndef __COORDINATOR_EVENT_SERVER_EVENT_HH__
#define __COORDINATOR_EVENT_SERVER_EVENT_HH__

#include "../socket/server_socket.hh"
#include "../../common/ds/packet_pool.hh"
#include "../../common/event/event.hh"

enum ServerEventType {
	SERVER_EVENT_TYPE_UNDEFINED,
	SERVER_EVENT_TYPE_REGISTER_RESPONSE_SUCCESS,
	SERVER_EVENT_TYPE_REGISTER_RESPONSE_FAILURE,
	SERVER_EVENT_TYPE_ANNOUNCE_SERVER_CONNECTED,
	SERVER_EVENT_TYPE_ANNOUNCE_SERVER_RECONSTRUCTED,
	SERVER_EVENT_TYPE_PENDING,
	SERVER_EVENT_TYPE_REQUEST_SEAL_CHUNKS,
	SERVER_EVENT_TYPE_REQUEST_FLUSH_CHUNKS,
	SERVER_EVENT_TYPE_REQUEST_SYNC_META,
	SERVER_EVENT_TYPE_PARITY_MIGRATE,
	SERVER_EVENT_TYPE_REQUEST_RELEASE_DEGRADED_LOCK,
	SERVER_EVENT_TYPE_RESPONSE_HEARTBEAT,
	SERVER_EVENT_TYPE_DISCONNECT,
	SERVER_EVENT_TYPE_TRIGGER_RECONSTRUCTION,
	SERVER_EVENT_TYPE_HANDLE_RECONSTRUCTION_REQUEST,
	SERVER_EVENT_TYPE_ACK_RECONSTRUCTION_SUCCESS,
	SERVER_EVENT_TYPE_ACK_RECONSTRUCTION_FAILURE
};

class ServerEvent : public Event {
public:
	ServerEventType type;
	uint16_t instanceId;
	uint32_t requestId;
	ServerSocket *socket;
	union {
		struct {
			ServerSocket *src;
			ServerSocket *dst;
			pthread_mutex_t *lock;
			pthread_cond_t *cond;
			std::unordered_set<ServerSocket *> *sockets;
		} reconstructed;
		struct {
			pthread_mutex_t *lock;
			pthread_cond_t *cond;
			bool *done;
		} degraded;
		struct {
			Packet *packet;
		} parity;
		bool *sync;
		struct {
			uint32_t timestamp;
			uint32_t sealed;
			uint32_t keys;
			bool isLast;
		} heartbeat;
		struct sockaddr_in addr;
	} message;

	void pending( ServerSocket *socket );
	void resRegister( ServerSocket *socket, uint16_t instanceId, uint32_t requestId, bool success = true );
	void announceSlaveConnected( ServerSocket *socket );
	void announceSlaveReconstructed(
		uint16_t instanceId, uint32_t requestId,
		pthread_mutex_t *lock, pthread_cond_t *cond, std::unordered_set<ServerSocket *> *sockets,
		ServerSocket *srcSocket, ServerSocket *dstSocket
	);
	void reqSealChunks( ServerSocket *socket );
	void reqFlushChunks( ServerSocket *socket );
	void reqSyncMeta( ServerSocket *socket, bool *sync );
	void reqReleaseDegradedLock( ServerSocket *socket, pthread_mutex_t *lock, pthread_cond_t *cond, bool *done );
	void syncRemappedData( ServerSocket *socket, Packet *packet );
	void resHeartbeat( ServerSocket *socket, uint32_t timestamp, uint32_t sealed, uint32_t keys, bool isLast );
	void disconnect( ServerSocket *socket );
	void triggerReconstruction( struct sockaddr_in addr );
	void handleReconstructionRequest( ServerSocket *socket );
	void ackCompletedReconstruction( ServerSocket *socket, uint16_t instanceId, uint32_t requestId, bool success );
};

#endif

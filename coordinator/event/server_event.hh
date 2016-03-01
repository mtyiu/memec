#ifndef __COORDINATOR_EVENT_SLAVE_EVENT_HH__
#define __COORDINATOR_EVENT_SLAVE_EVENT_HH__

#include "../socket/server_socket.hh"
#include "../../common/ds/packet_pool.hh"
#include "../../common/event/event.hh"

enum SlaveEventType {
	SLAVE_EVENT_TYPE_UNDEFINED,
	SLAVE_EVENT_TYPE_REGISTER_RESPONSE_SUCCESS,
	SLAVE_EVENT_TYPE_REGISTER_RESPONSE_FAILURE,
	SLAVE_EVENT_TYPE_ANNOUNCE_SLAVE_CONNECTED,
	SLAVE_EVENT_TYPE_ANNOUNCE_SLAVE_RECONSTRUCTED,
	SLAVE_EVENT_TYPE_PENDING,
	SLAVE_EVENT_TYPE_REQUEST_SEAL_CHUNKS,
	SLAVE_EVENT_TYPE_REQUEST_FLUSH_CHUNKS,
	SLAVE_EVENT_TYPE_REQUEST_SYNC_META,
	SLAVE_EVENT_TYPE_PARITY_MIGRATE,
	SLAVE_EVENT_TYPE_REQUEST_RELEASE_DEGRADED_LOCK,
	SLAVE_EVENT_TYPE_RESPONSE_HEARTBEAT,
	SLAVE_EVENT_TYPE_DISCONNECT,
	SLAVE_EVENT_TYPE_TRIGGER_RECONSTRUCTION,
	SLAVE_EVENT_TYPE_HANDLE_RECONSTRUCTION_REQUEST,
	SLAVE_EVENT_TYPE_ACK_RECONSTRUCTION_SUCCESS,
	SLAVE_EVENT_TYPE_ACK_RECONSTRUCTION_FAILURE
};

class SlaveEvent : public Event {
public:
	SlaveEventType type;
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

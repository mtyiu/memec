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
	SERVER_EVENT_TYPE_ACK_RECONSTRUCTION_FAILURE,
	SERVER_EVENT_TYPE_ADD_NEW_SERVER_FIRST,
	SERVER_EVENT_TYPE_ADD_NEW_SERVER,
	SERVER_EVENT_TYPE_UPDATE_STRIPE_LIST,
	SERVER_EVENT_TYPE_MIGRATE
};

class ServerEvent : public Event<ServerSocket> {
public:
	ServerEventType type;
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
		struct {
			uint32_t *requestIdPtr;
			uint8_t nameLen;
			char *name;
			ServerSocket *socket;
			LOCK_T *lock;
			pthread_cond_t *cond;
			uint32_t *count;
			uint32_t total;
		} add;
		struct {
			LOCK_T *lock;
			pthread_cond_t *cond;
			uint32_t *count;
			uint32_t total;
			uint32_t *numMigrated;
		} migrate;
		bool isMigrating;
	} message;

	inline void pending( ServerSocket *socket ) {
		this->type = SERVER_EVENT_TYPE_PENDING;
		this->socket = socket;
	}

	inline void resRegister( ServerSocket *socket, uint16_t instanceId, uint32_t requestId, bool success = true ) {
		this->type = success ? SERVER_EVENT_TYPE_REGISTER_RESPONSE_SUCCESS : SERVER_EVENT_TYPE_REGISTER_RESPONSE_FAILURE;
		this->set( instanceId, requestId, socket );
	}

	inline void announceServerConnected( ServerSocket *socket ) {
		this->type = SERVER_EVENT_TYPE_ANNOUNCE_SERVER_CONNECTED;
		this->socket = socket;
	}

	inline void announceServerReconstructed(
		uint16_t instanceId, uint32_t requestId,
		pthread_mutex_t *lock, pthread_cond_t *cond, std::unordered_set<ServerSocket *> *sockets,
		ServerSocket *srcSocket, ServerSocket *dstSocket
	) {
		this->type = SERVER_EVENT_TYPE_ANNOUNCE_SERVER_RECONSTRUCTED;
		this->set( instanceId, requestId );
		this->message.reconstructed = {
			.src = srcSocket,
			.dst = dstSocket,
			.lock = lock,
			.cond = cond,
			.sockets = sockets
		};
	}

	inline void reqSealChunks( ServerSocket *socket ) {
		this->type = SERVER_EVENT_TYPE_REQUEST_SEAL_CHUNKS;
		this->socket = socket;
	}

	inline void reqFlushChunks( ServerSocket *socket ) {
		this->type = SERVER_EVENT_TYPE_REQUEST_FLUSH_CHUNKS;
		this->socket = socket;
	}

	inline void reqSyncMeta( ServerSocket *socket, bool *sync ) {
		this->type = SERVER_EVENT_TYPE_REQUEST_SYNC_META;
		this->socket = socket;
		this->message.sync = sync;
	}

	inline void reqReleaseDegradedLock(
		ServerSocket *socket,
		pthread_mutex_t *lock, pthread_cond_t *cond, bool *done
	) {
		this->type = SERVER_EVENT_TYPE_REQUEST_RELEASE_DEGRADED_LOCK;
		this->socket = socket;
		this->message.degraded = {
			.lock = lock,
			.cond = cond,
			.done = done
		};
	}

	inline void syncRemappedData( ServerSocket *socket, Packet *packet ) {
		this->type = SERVER_EVENT_TYPE_PARITY_MIGRATE;
		this->socket = socket;
		this->message.parity.packet = packet;
	}

	inline void resHeartbeat(
		ServerSocket *socket,
		uint32_t timestamp, uint32_t sealed, uint32_t keys, bool isLast
	) {
		this->type = SERVER_EVENT_TYPE_RESPONSE_HEARTBEAT;
		this->socket = socket;
		this->message.heartbeat = {
			.timestamp = timestamp,
			.sealed = sealed,
			.keys = keys,
			.isLast = isLast
		};
	}

	inline void disconnect( ServerSocket *socket ) {
		this->type = SERVER_EVENT_TYPE_DISCONNECT;
		this->socket = socket;
	}

	inline void triggerReconstruction( struct sockaddr_in addr ) {
		this->type = SERVER_EVENT_TYPE_TRIGGER_RECONSTRUCTION;
		this->message.addr = addr;
	}

	inline void handleReconstructionRequest( ServerSocket *socket ) {
		this->type = SERVER_EVENT_TYPE_HANDLE_RECONSTRUCTION_REQUEST;
		this->socket = socket;
	}

	inline void ackCompletedReconstruction( ServerSocket *socket, uint16_t instanceId, uint32_t requestId, bool success ) {
		this->type = success ? SERVER_EVENT_TYPE_ACK_RECONSTRUCTION_SUCCESS : SERVER_EVENT_TYPE_ACK_RECONSTRUCTION_FAILURE;
		this->set( instanceId, requestId, socket );
	}

	inline void addNewServer( uint32_t *requestIdPtr, uint8_t nameLen, char *name, ServerSocket *socket, LOCK_T *lock, pthread_cond_t *cond, uint32_t *count, uint32_t total, bool first ) {
		this->type = first ? SERVER_EVENT_TYPE_ADD_NEW_SERVER_FIRST : SERVER_EVENT_TYPE_ADD_NEW_SERVER;
		this->message.add = {
			.requestIdPtr = requestIdPtr,
			.nameLen = nameLen,
			.name = name,
			.socket = socket,
			.lock = lock,
			.cond = cond,
			.count = count,
			.total = total
		};
	}

	inline void updateStripeList( bool isMigrating = true ) {
		this->type = SERVER_EVENT_TYPE_UPDATE_STRIPE_LIST;
		this->message.isMigrating = isMigrating;
	}

	inline void migrate( LOCK_T *lock, pthread_cond_t *cond, uint32_t *count, uint32_t total, uint32_t *numMigrated ) {
		this->type = SERVER_EVENT_TYPE_MIGRATE;
		this->message.migrate = {
			.lock = lock,
			.cond = cond,
			.count = count,
			.total = total,
			.numMigrated = numMigrated
		};
	}
};

#endif

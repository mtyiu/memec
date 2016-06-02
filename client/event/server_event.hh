#ifndef __CLIENT_EVENT_SERVER_EVENT_HH__
#define __CLIENT_EVENT_SERVER_EVENT_HH__

#include "../protocol/protocol.hh"
#include "../socket/server_socket.hh"
#include "../../common/ds/packet_pool.hh"
#include "../../common/event/event.hh"

enum ServerEventType {
	SERVER_EVENT_TYPE_UNDEFINED,
	SERVER_EVENT_TYPE_REGISTER_REQUEST,
	SERVER_EVENT_TYPE_SEND,
	SERVER_EVENT_TYPE_SYNC_METADATA,
	SERVER_EVENT_TYPE_ACK_PARITY_DELTA,
	SERVER_EVENT_TYPE_REVERT_DELTA,
	SERVER_EVENT_TYPE_PENDING
};

class ServerEvent : public Event<ServerSocket> {
public:
	ServerEventType type;
	uint32_t timestamp;
	union {
		struct {
			uint32_t addr;
			uint16_t port;
		} address;
		struct {
			Packet *packet;
		} send;
		struct {
			std::vector<uint32_t> *timestamps;
			std::vector<Key> *requests;
			uint16_t targetId;
			pthread_cond_t *condition;
			LOCK_T *lock;
			uint32_t *counter;
		} ack;
	} message;

	inline void reqRegister( ServerSocket *socket, uint32_t addr, uint16_t port ) {
		this->type = SERVER_EVENT_TYPE_REGISTER_REQUEST;
		this->socket = socket;
		this->message.address.addr = addr;
		this->message.address.port = port;
	}

	inline void send( ServerSocket *socket, Packet *packet ) {
		this->type = SERVER_EVENT_TYPE_SEND;
		this->socket = socket;
		this->message.send.packet = packet;
	}

	inline void syncMetadata( ServerSocket *socket ) {
		this->type = SERVER_EVENT_TYPE_SYNC_METADATA;
		this->socket = socket;
	}

	inline void ackParityDelta(
		ServerSocket *socket, std::vector<uint32_t> timestamps, uint16_t targetId,
		pthread_cond_t *condition, LOCK_T *lock, uint32_t *counter
	) {
		this->type = SERVER_EVENT_TYPE_ACK_PARITY_DELTA;
		this->socket = socket;
		this->message.ack = {
			.timestamps = timestamps.empty() ? 0 : new std::vector<uint32_t>( timestamps ),
			.requests = 0,
			.targetId = targetId,
			.condition = condition,
			.lock = lock,
			.counter = counter
		};
	}

	inline void revertDelta( ServerSocket *socket, std::vector<uint32_t> timestamps, std::vector<Key> requests, uint16_t targetId, pthread_cond_t *condition, LOCK_T *lock, uint32_t *counter ) {
		this->type = SERVER_EVENT_TYPE_REVERT_DELTA;
		this->socket = socket;
		this->message.ack = {
			.timestamps = ( timestamps.empty() )? 0 : new std::vector<uint32_t>( timestamps ),
			.requests = ( requests.empty() )? 0 : new std::vector<Key>( requests ),
			.targetId = targetId, // source data server
			.condition = condition,
			.lock = lock,
			.counter = counter
		};
	}

	inline void pending( ServerSocket *socket ) {
		this->type = SERVER_EVENT_TYPE_PENDING;
		this->socket = socket;
	}
};

#endif

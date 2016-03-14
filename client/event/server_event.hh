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

class ServerEvent : public Event {
public:
	ServerEventType type;
	ServerSocket *socket;
	uint16_t instanceId;
	uint32_t requestId;
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

	void reqRegister( ServerSocket *socket, uint32_t addr, uint16_t port );
	void send( ServerSocket *socket, Packet *packet );
	void syncMetadata( ServerSocket *socket );
	void ackParityDelta( ServerSocket *socket, std::vector<uint32_t> timestamps, uint16_t targetId, pthread_cond_t *condition, LOCK_T *lock, uint32_t *counter );
	void revertDelta( ServerSocket *socket, std::vector<uint32_t> timestamps, std::vector<Key> requests, uint16_t targetId, pthread_cond_t *condition, LOCK_T *lock, uint32_t *counter );
	void pending( ServerSocket *socket );
};

#endif

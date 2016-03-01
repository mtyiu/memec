#ifndef __MASTER_EVENT_SLAVE_EVENT_HH__
#define __MASTER_EVENT_SLAVE_EVENT_HH__

#include "../protocol/protocol.hh"
#include "../socket/server_socket.hh"
#include "../../common/ds/packet_pool.hh"
#include "../../common/event/event.hh"

enum SlaveEventType {
	SLAVE_EVENT_TYPE_UNDEFINED,
	SLAVE_EVENT_TYPE_REGISTER_REQUEST,
	SLAVE_EVENT_TYPE_SEND,
	SLAVE_EVENT_TYPE_SYNC_METADATA,
	SLAVE_EVENT_TYPE_ACK_PARITY_DELTA,
	SLAVE_EVENT_TYPE_REVERT_DELTA,
	SLAVE_EVENT_TYPE_PENDING
};

class SlaveEvent : public Event {
public:
	SlaveEventType type;
	SlaveSocket *socket;
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

	void reqRegister( SlaveSocket *socket, uint32_t addr, uint16_t port );
	void send( SlaveSocket *socket, Packet *packet );
	void syncMetadata( SlaveSocket *socket );
	void ackParityDelta( SlaveSocket *socket, std::vector<uint32_t> timestamps, uint16_t targetId, pthread_cond_t *condition, LOCK_T *lock, uint32_t *counter );
	void revertDelta( SlaveSocket *socket, std::vector<uint32_t> timestamps, std::vector<Key> requests, uint16_t targetId, pthread_cond_t *condition, LOCK_T *lock, uint32_t *counter );
	void pending( SlaveSocket *socket );
};

#endif

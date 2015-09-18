#ifndef __MASTER_EVENT_SLAVE_EVENT_HH__
#define __MASTER_EVENT_SLAVE_EVENT_HH__

#include "../protocol/protocol.hh"
#include "../socket/slave_socket.hh"
#include "../../common/ds/packet_pool.hh"
#include "../../common/event/event.hh"

enum SlaveEventType {
	SLAVE_EVENT_TYPE_UNDEFINED,
	SLAVE_EVENT_TYPE_REGISTER_REQUEST,
	SLAVE_EVENT_TYPE_SEND,
	SLAVE_EVENT_TYPE_PENDING
};

class SlaveEvent : public Event {
public:
	SlaveEventType type;
	uint32_t id;
	SlaveSocket *socket;
	union {
		struct {
			uint32_t addr;
			uint16_t port;
		} address;
		struct {
			Packet *packet;
		} send;
	} message;

	void reqRegister( SlaveSocket *socket, uint32_t addr, uint16_t port );
	void send( SlaveSocket *socket, Packet *packet );
	void pending( SlaveSocket *socket );
};

#endif

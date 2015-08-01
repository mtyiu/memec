#ifndef __MASTER_EVENT_SLAVE_EVENT_HH__
#define __MASTER_EVENT_SLAVE_EVENT_HH__

#include "../protocol/protocol.hh"
#include "../socket/slave_socket.hh"
#include "../../common/event/event.hh"

enum SlaveEventType {
	SLAVE_EVENT_TYPE_UNDEFINED,
	SLAVE_EVENT_TYPE_REGISTER_REQUEST,
	SLAVE_EVENT_TYPE_SEND, // send data in the provided protocol buffer
	SLAVE_EVENT_TYPE_PENDING
};

class SlaveEvent : public Event {
public:
	SlaveEventType type;
	SlaveSocket *socket;
	union {
		struct {
			size_t size;
			size_t index;
			MasterProtocol *protocol;
		} send;
	} message;

	void reqRegister( SlaveSocket *socket );
	void send( SlaveSocket *socket, MasterProtocol *protocol, size_t size, size_t index );
	void pending( SlaveSocket *socket );
};

#endif

#ifndef __SLAVE_EVENT_COORDINATOR_EVENT_HH__
#define __SLAVE_EVENT_COORDINATOR_EVENT_HH__

#include "../socket/coordinator_socket.hh"
#include "../../common/event/event.hh"

enum CoordinatorEventType {
	COORDINATOR_EVENT_TYPE_REGISTER_REQUEST,
	COORDINATOR_EVENT_TYPE_SYNC,
	COORDINATOR_EVENT_TYPE_REMAP_SYNC,
	COORDINATOR_EVENT_TYPE_PENDING
};

class CoordinatorEvent : public Event {
public:
	CoordinatorEventType type;
	CoordinatorSocket *socket;
	uint32_t id;
	union {
		struct {
			uint32_t addr;
			uint16_t port;
		} address;
	} message;

	void reqRegister( CoordinatorSocket *socket, uint32_t addr, uint16_t port );
	void sync( CoordinatorSocket *socket, uint32_t id = 0 );
	void syncRemap( CoordinatorSocket *socket );
	void pending( CoordinatorSocket *socket );
};

#endif

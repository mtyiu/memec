#ifndef __MASTER_EVENT_COORDINATOR_EVENT_HH__
#define __MASTER_EVENT_COORDINATOR_EVENT_HH__

#include "../socket/coordinator_socket.hh"
#include "../../common/ds/array_map.hh"
#include "../../common/ds/latency.hh"
#include "../../common/event/event.hh"

enum CoordinatorEventType {
	COORDINATOR_EVENT_TYPE_REGISTER_REQUEST,
	COORDINATOR_EVENT_TYPE_PUSH_LOAD_STATS,
	COORDINATOR_EVENT_TYPE_PENDING
};

class CoordinatorEvent : public Event {
public:
	CoordinatorEventType type;
	CoordinatorSocket *socket;
	union {
		struct {
			uint32_t addr;
			uint16_t port;
		} address;
		struct {
			ArrayMap< struct sockaddr_in, Latency >* slaveGetLatency;
			ArrayMap< struct sockaddr_in, Latency >* slaveSetLatency;
		} loading;
	} message;

	void reqRegister( CoordinatorSocket *socket, uint32_t addr, uint16_t port );
	void reqSendLoadStats(
		CoordinatorSocket *socket,
		ArrayMap< struct sockaddr_in, Latency > *slaveGetLatency,
		ArrayMap< struct sockaddr_in, Latency > *slaveSetLatency
	);
	void pending( CoordinatorSocket *socket );
};

#endif

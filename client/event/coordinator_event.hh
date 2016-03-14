#ifndef __CLIENT_EVENT_COORDINATOR_EVENT_HH__
#define __CLIENT_EVENT_COORDINATOR_EVENT_HH__

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
	uint16_t instanceId;
	uint32_t requestId;
	union {
		struct {
			uint32_t addr;
			uint16_t port;
		} address;
		struct {
			ArrayMap< struct sockaddr_in, Latency >* serverGetLatency;
			ArrayMap< struct sockaddr_in, Latency >* serverSetLatency;
		} loading;
	} message;

	void reqRegister( CoordinatorSocket *socket, uint32_t addr, uint16_t port );
	void reqSendLoadStats(
		CoordinatorSocket *socket,
		ArrayMap< struct sockaddr_in, Latency > *serverGetLatency,
		ArrayMap< struct sockaddr_in, Latency > *serverSetLatency
	);
	void pending( CoordinatorSocket *socket );
};

#endif

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

class CoordinatorEvent : public Event<CoordinatorSocket> {
public:
	CoordinatorEventType type;
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

	inline void reqRegister( CoordinatorSocket *socket, uint32_t addr, uint16_t port ) {
		this->type = COORDINATOR_EVENT_TYPE_REGISTER_REQUEST;
		this->socket = socket;
		this->message.address = {
			.addr = addr,
			.port = port
		};
	}

	inline void reqSendLoadStats(
		CoordinatorSocket *socket,
		ArrayMap< struct sockaddr_in, Latency > *serverGetLatency,
		ArrayMap< struct sockaddr_in, Latency > *serverSetLatency
	) {
		this->type = COORDINATOR_EVENT_TYPE_PUSH_LOAD_STATS;
		this->socket = socket;
		this->message.loading = {
			.serverGetLatency = serverGetLatency,
			.serverSetLatency = serverSetLatency
		};
	}

	inline void pending( CoordinatorSocket *socket ) {
		this->type = COORDINATOR_EVENT_TYPE_PENDING;
		this->socket = socket;
	}
};

#endif

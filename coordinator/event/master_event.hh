#ifndef __COORDINATOR_EVENT_MASTER_EVENT_HH__
#define __COORDINATOR_EVENT_MASTER_EVENT_HH__

#include "../socket/master_socket.hh"
#include "../../common/ds/latency.hh"
#include "../../common/event/event.hh"

enum MasterEventType {
	MASTER_EVENT_TYPE_UNDEFINED,
	MASTER_EVENT_TYPE_REGISTER_RESPONSE_SUCCESS,
	MASTER_EVENT_TYPE_REGISTER_RESPONSE_FAILURE,
	MASTER_EVENT_TYPE_PUSH_LOADING_STATS,
	MASTER_EVENT_TYPE_PENDING
};

class MasterEvent : public Event {
public:
	MasterEventType type;
	uint32_t id;
	MasterSocket *socket;
	union {
		struct {
			ArrayMap<struct sockaddr_in, Latency> *slaveGetLatency;
			ArrayMap<struct sockaddr_in, Latency> *slaveSetLatency;
		} slaveLoading;
	} message;

	void resRegister( MasterSocket *socket, uint32_t id, bool success = true );
	void reqPushLoadStats ( MasterSocket *socket, ArrayMap<struct sockaddr_in, Latency> *slaveGetLatency, 
			ArrayMap<struct sockaddr_in, Latency> *slaveSetLatency );
	void pending( MasterSocket *socket );
};

#endif

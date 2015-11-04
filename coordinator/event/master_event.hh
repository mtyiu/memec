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
	MASTER_EVENT_TYPE_SWITCH_PHASE,
	MASTER_EVENT_TYPE_FORWARD_REMAPPING_RECORDS,
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
			std::set<struct sockaddr_in> *overloadedSlaveSet;
		} slaveLoading;
		struct {
			bool toRemap;
			std::vector<struct sockaddr_in> *slaves;
		} remap;
		struct {
			size_t prevSize;
			char *data;
		} forward;
	} message;

	void resRegister( MasterSocket *socket, uint32_t id, bool success = true );
	void reqPushLoadStats ( MasterSocket *socket, ArrayMap<struct sockaddr_in, Latency> *slaveGetLatency, 
			ArrayMap<struct sockaddr_in, Latency> *slaveSetLatency, std::set<struct sockaddr_in> *slaveSet );
	void switchPhase( bool toRemap );
	void pending( MasterSocket *socket );
};

#endif

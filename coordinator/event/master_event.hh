#ifndef __COORDINATOR_EVENT_MASTER_EVENT_HH__
#define __COORDINATOR_EVENT_MASTER_EVENT_HH__

#include <set>
#include "../socket/master_socket.hh"
#include "../../common/ds/latency.hh"
#include "../../common/ds/key.hh"
#include "../../common/ds/metadata.hh"
#include "../../common/event/event.hh"

enum MasterEventType {
	MASTER_EVENT_TYPE_UNDEFINED,
	// REGISTER
	MASTER_EVENT_TYPE_REGISTER_RESPONSE_SUCCESS,
	MASTER_EVENT_TYPE_REGISTER_RESPONSE_FAILURE,
	// LOADING_STAT
	MASTER_EVENT_TYPE_PUSH_LOADING_STATS,
	// REMAPPING_PHASE_SWITCH
	MASTER_EVENT_TYPE_SWITCH_PHASE,
	// REMAPPING_RECORDS
	MASTER_EVENT_TYPE_FORWARD_REMAPPING_RECORDS,
	// REMAPPING_SET_LOCK
	MASTER_EVENT_TYPE_REMAPPING_SET_LOCK_RESPONSE_SUCCESS,
	MASTER_EVENT_TYPE_REMAPPING_SET_LOCK_RESPONSE_FAILURE,
	// PENDING
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
			Key key;
			uint32_t listId;
			uint32_t chunkId;
			uint32_t isRemapped;
		} remap;
		struct {
			size_t prevSize;
			char *data;
		} forward;
	} message;

	void resRegister( MasterSocket *socket, uint32_t id, bool success = true );
	void reqPushLoadStats ( MasterSocket *socket, ArrayMap<struct sockaddr_in, Latency> *slaveGetLatency, 
			ArrayMap<struct sockaddr_in, Latency> *slaveSetLatency, std::set<struct sockaddr_in> *slaveSet );
	void switchPhase( bool toRemap, std::set<struct sockaddr_in> slaves );
	// REMAPPING_SET_LOCK
	void resRemappingSetLock( MasterSocket *socket, uint32_t id, bool isRemapped, Key &key, RemappingRecord &remappingRecord, bool success );
	void pending( MasterSocket *socket );
};

#endif

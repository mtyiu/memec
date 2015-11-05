#ifndef __COORDINATOR_EVENT_MASTER_EVENT_HH__
#define __COORDINATOR_EVENT_MASTER_EVENT_HH__

#include "../socket/master_socket.hh"
#include "../../common/ds/key.hh"
#include "../../common/ds/latency.hh"
#include "../../common/event/event.hh"

enum MasterEventType {
	MASTER_EVENT_TYPE_UNDEFINED,
	MASTER_EVENT_TYPE_REGISTER_RESPONSE_SUCCESS,
	MASTER_EVENT_TYPE_REGISTER_RESPONSE_FAILURE,
	MASTER_EVENT_TYPE_PUSH_LOADING_STATS,
	MASTER_EVENT_TYPE_SWITCH_PHASE,
	MASTER_EVENT_TYPE_FORWARD_REMAPPING_RECORDS,
	// Degraded operation
	MASTER_EVENT_TYPE_DEGRADED_LOCK_RESPONSE_IS_LOCKED,
	MASTER_EVENT_TYPE_DEGRADED_LOCK_RESPONSE_WAS_LOCKED,
	MASTER_EVENT_TYPE_DEGRADED_LOCK_RESPONSE_REMAPPED,
	MASTER_EVENT_TYPE_DEGRADED_LOCK_RESPONSE_NOT_FOUND,
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
		} remap;
		struct {
			size_t prevSize;
			char *data;
		} forward;
		struct {
			Key key;
			uint32_t srcListId, srcStripeId, srcChunkId;
			uint32_t dstListId, dstChunkId;
		} degradedLock;
	} message;

	void resRegister( MasterSocket *socket, uint32_t id, bool success = true );
	void reqPushLoadStats (
		MasterSocket *socket,
		ArrayMap<struct sockaddr_in, Latency> *slaveGetLatency,
		ArrayMap<struct sockaddr_in, Latency> *slaveSetLatency,
		std::set<struct sockaddr_in> *slaveSet
	);
	void switchPhase( bool toRemap );
	// Degraded lock
	void resDegradedLock(
		MasterSocket *socket, uint32_t id, Key &key, bool isLocked,
		uint32_t srcListId, uint32_t srcStripeId, uint32_t srcChunkId,
		uint32_t dstListId, uint32_t dstChunkId
	);
	void resDegradedLock(
		MasterSocket *socket, uint32_t id, Key &key,
		uint32_t listId, uint32_t chunkId
	);
	void resDegradedLock( MasterSocket *socket, uint32_t id, Key &key );
	// Pending
	void pending( MasterSocket *socket );
};

#endif

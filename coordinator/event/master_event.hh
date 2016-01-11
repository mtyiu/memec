#ifndef __COORDINATOR_EVENT_MASTER_EVENT_HH__
#define __COORDINATOR_EVENT_MASTER_EVENT_HH__

#include <climits>
#include <set>
#include "../socket/master_socket.hh"
#include "../socket/slave_socket.hh"
#include "../../common/ds/key.hh"
#include "../../common/ds/latency.hh"
#include "../../common/ds/key.hh"
#include "../../common/ds/metadata.hh"
#include "../../common/ds/packet_pool.hh"
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
	// Degraded operation
	MASTER_EVENT_TYPE_DEGRADED_LOCK_RESPONSE_IS_LOCKED,
	MASTER_EVENT_TYPE_DEGRADED_LOCK_RESPONSE_WAS_LOCKED,
	MASTER_EVENT_TYPE_DEGRADED_LOCK_RESPONSE_NOT_LOCKED,
	MASTER_EVENT_TYPE_DEGRADED_LOCK_RESPONSE_REMAPPED,
	MASTER_EVENT_TYPE_DEGRADED_LOCK_RESPONSE_NOT_FOUND,
	// REMAPPING_SET_LOCK
	MASTER_EVENT_TYPE_REMAPPING_SET_LOCK_RESPONSE_SUCCESS,
	MASTER_EVENT_TYPE_REMAPPING_SET_LOCK_RESPONSE_FAILURE,
	// Recovery
	MASTER_EVENT_TYPE_ANNOUNCE_SLAVE_RECONSTRUCTED,
	// PENDING
	MASTER_EVENT_TYPE_PENDING
};

class MasterEvent : public Event {
public:
	MasterEventType type;
	uint16_t instanceId;
	uint32_t requestId;
	MasterSocket *socket;
	union {
		struct {
			ArrayMap<struct sockaddr_in, Latency> *slaveGetLatency;
			ArrayMap<struct sockaddr_in, Latency> *slaveSetLatency;
			std::set<struct sockaddr_in> *overloadedSlaveSet;
		} slaveLoading;
		struct {
			bool toRemap;
			bool isCrashed;
			std::vector<struct sockaddr_in> *slaves;
		} switchPhase;
		struct {
			uint32_t *original;
			uint32_t *remapped;
			uint32_t remappedCount;
			Key key;
		} remap;
		struct {
			bool isSealed;
			uint32_t stripeId;
			uint32_t reconstructedCount;
			uint32_t remappedCount;
			uint32_t ongoingAtChunk;
			uint32_t *original;
			uint32_t *reconstructed;
			uint32_t *remapped;
			Key key;
		} degradedLock;
		struct {
			SlaveSocket *src;
			SlaveSocket *dst;
		} reconstructed;
	} message;

	void resRegister( MasterSocket *socket, uint16_t instanceId, uint32_t requestId, bool success = true );

	void reqPushLoadStats (
		MasterSocket *socket,
		ArrayMap<struct sockaddr_in, Latency> *slaveGetLatency,
		ArrayMap<struct sockaddr_in, Latency> *slaveSetLatency,
		std::set<struct sockaddr_in> *slaveSet
	);
	void switchPhase( bool toRemap, std::set<struct sockaddr_in> slaves, bool isCrashed = false );
	// Degraded lock
	void resDegradedLock(
		MasterSocket *socket, uint16_t instanceId, uint32_t requestId,
		Key &key, bool isLocked, bool isSealed,
		uint32_t stripeId,
		uint32_t *original, uint32_t *reconstructed, uint32_t reconstructedCount,
		uint32_t ongoingAtChunk
	);
	void resDegradedLock(
		MasterSocket *socket, uint16_t instanceId, uint32_t requestId,
		Key &key, bool exist
	);
	void resDegradedLock(
		MasterSocket *socket, uint16_t instanceId, uint32_t requestId,
		Key &key,
		uint32_t *original, uint32_t *remapped, uint32_t remappedCount
	);
	// REMAPPING_SET_LOCK
	void resRemappingSetLock(
		MasterSocket *socket, uint16_t instanceId, uint32_t requestId, bool success,
		uint32_t *original, uint32_t *remapped, uint32_t remappedCount, Key &key
	);
	// Recovery
	void announceSlaveReconstructed( SlaveSocket *srcSocket, SlaveSocket *dstSocket );
	// Pending
	void pending( MasterSocket *socket );
};

#endif

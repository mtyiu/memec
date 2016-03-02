#ifndef __COORDINATOR_EVENT_CLIENT_EVENT_HH__
#define __COORDINATOR_EVENT_CLIENT_EVENT_HH__

#include <climits>
#include <set>
#include "../socket/client_socket.hh"
#include "../socket/server_socket.hh"
#include "../../common/ds/key.hh"
#include "../../common/ds/latency.hh"
#include "../../common/ds/key.hh"
#include "../../common/ds/metadata.hh"
#include "../../common/ds/packet_pool.hh"
#include "../../common/event/event.hh"

enum ClientEventType {
	CLIENT_EVENT_TYPE_UNDEFINED,
	// REGISTER
	CLIENT_EVENT_TYPE_REGISTER_RESPONSE_SUCCESS,
	CLIENT_EVENT_TYPE_REGISTER_RESPONSE_FAILURE,
	// LOADING_STAT
	CLIENT_EVENT_TYPE_PUSH_LOADING_STATS,
	// REMAPPING_PHASE_SWITCH
	CLIENT_EVENT_TYPE_SWITCH_PHASE,
	// Degraded operation
	CLIENT_EVENT_TYPE_DEGRADED_LOCK_RESPONSE_IS_LOCKED,
	CLIENT_EVENT_TYPE_DEGRADED_LOCK_RESPONSE_WAS_LOCKED,
	CLIENT_EVENT_TYPE_DEGRADED_LOCK_RESPONSE_NOT_LOCKED,
	CLIENT_EVENT_TYPE_DEGRADED_LOCK_RESPONSE_REMAPPED,
	CLIENT_EVENT_TYPE_DEGRADED_LOCK_RESPONSE_NOT_FOUND,
	// REMAPPING_SET_LOCK
	CLIENT_EVENT_TYPE_REMAPPING_SET_LOCK_RESPONSE_SUCCESS,
	CLIENT_EVENT_TYPE_REMAPPING_SET_LOCK_RESPONSE_FAILURE,
	// Recovery
	CLIENT_EVENT_TYPE_ANNOUNCE_SERVER_RECONSTRUCTED,
	// PENDING
	CLIENT_EVENT_TYPE_PENDING
};

class ClientEvent : public Event {
public:
	ClientEventType type;
	uint16_t instanceId;
	uint32_t requestId;
	ClientSocket *socket;
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
			bool forced;
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
			uint32_t dataChunkId;
			uint32_t dataChunkCount;
			uint32_t reconstructedCount;
			uint32_t remappedCount;
			uint32_t ongoingAtChunk;
			uint32_t *original;
			uint32_t *reconstructed;
			uint32_t *remapped;
			uint8_t numSurvivingChunkIds;
			uint32_t *survivingChunkIds;
			Key key;
		} degradedLock;
		struct {
			ServerSocket *src;
			ServerSocket *dst;
		} reconstructed;
	} message;

	void resRegister( ClientSocket *socket, uint16_t instanceId, uint32_t requestId, bool success = true );

	void reqPushLoadStats (
		ClientSocket *socket,
		ArrayMap<struct sockaddr_in, Latency> *slaveGetLatency,
		ArrayMap<struct sockaddr_in, Latency> *slaveSetLatency,
		std::set<struct sockaddr_in> *slaveSet
	);
	void switchPhase( bool toRemap, std::set<struct sockaddr_in> slaves, bool isCrashed = false, bool forced = false );
	// Degraded lock
	void resDegradedLock(
		ClientSocket *socket, uint16_t instanceId, uint32_t requestId,
		Key &key, bool isLocked, bool isSealed,
		uint32_t stripeId, uint32_t dataChunkId, uint32_t dataChunkCount,
		uint32_t *original, uint32_t *reconstructed, uint32_t reconstructedCount,
		uint32_t ongoingAtChunk, uint8_t numSurvivingChunkIds, uint32_t *survivingChunkIds
	);
	void resDegradedLock(
		ClientSocket *socket, uint16_t instanceId, uint32_t requestId,
		Key &key, bool exist
	);
	void resDegradedLock(
		ClientSocket *socket, uint16_t instanceId, uint32_t requestId,
		Key &key,
		uint32_t *original, uint32_t *remapped, uint32_t remappedCount
	);
	// REMAPPING_SET_LOCK
	void resRemappingSetLock(
		ClientSocket *socket, uint16_t instanceId, uint32_t requestId, bool success,
		uint32_t *original, uint32_t *remapped, uint32_t remappedCount, Key &key
	);
	// Recovery
	void announceSlaveReconstructed( ServerSocket *srcSocket, ServerSocket *dstSocket );
	// Pending
	void pending( ClientSocket *socket );
};

#endif

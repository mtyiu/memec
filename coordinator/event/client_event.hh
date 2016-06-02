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
	// DEGRADED_SET_LOCK
	CLIENT_EVENT_TYPE_DEGRADED_SET_LOCK_RESPONSE_SUCCESS,
	CLIENT_EVENT_TYPE_DEGRADED_SET_LOCK_RESPONSE_FAILURE,
	// Recovery
	CLIENT_EVENT_TYPE_ANNOUNCE_SERVER_RECONSTRUCTED,
	// PENDING
	CLIENT_EVENT_TYPE_PENDING
};

class ClientEvent : public Event<ClientSocket> {
public:
	ClientEventType type;
	union {
		struct {
			ArrayMap<struct sockaddr_in, Latency> *serverGetLatency;
			ArrayMap<struct sockaddr_in, Latency> *serverSetLatency;
			std::set<struct sockaddr_in> *overloadedServerSet;
		} serverLoading;
		struct {
			bool toRemap;
			bool isCrashed;
			std::vector<struct sockaddr_in> *servers;
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

	inline void resRegister( ClientSocket *socket, uint16_t instanceId, uint32_t requestId, bool success = true ) {
		this->type = success ? CLIENT_EVENT_TYPE_REGISTER_RESPONSE_SUCCESS : CLIENT_EVENT_TYPE_REGISTER_RESPONSE_FAILURE;
		this->set( instanceId, requestId, socket );
	}

	inline void reqPushLoadStats(
		ClientSocket *socket,
		ArrayMap<struct sockaddr_in, Latency> *serverGetLatency,
		ArrayMap<struct sockaddr_in, Latency> *serverSetLatency,
		std::set<struct sockaddr_in> *overloadedServerSet
	) {
		this->type = CLIENT_EVENT_TYPE_PUSH_LOADING_STATS;
		this->socket = socket;
		this->message.serverLoading.serverGetLatency = serverGetLatency;
		this->message.serverLoading.serverSetLatency = serverSetLatency;
		this->message.serverLoading.overloadedServerSet = overloadedServerSet;
	}

	inline void switchPhase(
		bool toRemap, std::set<struct sockaddr_in> servers,
		bool isCrashed = false, bool forced = false
	) {
		this->type = CLIENT_EVENT_TYPE_SWITCH_PHASE;
		this->message.switchPhase.toRemap = toRemap;
		this->message.switchPhase.isCrashed = isCrashed;
		this->message.switchPhase.servers = new std::vector<struct sockaddr_in>( servers.begin(), servers.end() );
		this->message.switchPhase.forced = forced;
	}

	inline void resDegradedLock(
		ClientSocket *socket, uint16_t instanceId, uint32_t requestId,
		Key &key, bool isLocked, bool isSealed,
		uint32_t stripeId, uint32_t dataChunkId, uint32_t dataChunkCount,
		uint32_t *original, uint32_t *reconstructed, uint32_t reconstructedCount,
		uint32_t ongoingAtChunk, uint8_t numSurvivingChunkIds, uint32_t *survivingChunkIds
	) {
		this->type = isLocked ? CLIENT_EVENT_TYPE_DEGRADED_LOCK_RESPONSE_IS_LOCKED : CLIENT_EVENT_TYPE_DEGRADED_LOCK_RESPONSE_WAS_LOCKED;
		this->set( instanceId, requestId );
		this->socket = socket;
		this->message.degradedLock.key = key;
		this->message.degradedLock.isSealed = isSealed;
		this->message.degradedLock.stripeId = stripeId;
		this->message.degradedLock.dataChunkId = dataChunkId;
		this->message.degradedLock.dataChunkCount = dataChunkCount;
		this->message.degradedLock.reconstructedCount = reconstructedCount;
		this->message.degradedLock.original = original;
		this->message.degradedLock.reconstructed = reconstructed;
		this->message.degradedLock.ongoingAtChunk = ongoingAtChunk;
		this->message.degradedLock.numSurvivingChunkIds = numSurvivingChunkIds;
		this->message.degradedLock.survivingChunkIds = survivingChunkIds;
	};

	inline void resDegradedLock(
		ClientSocket *socket, uint16_t instanceId, uint32_t requestId,
		Key &key,
		uint32_t *original, uint32_t *remapped, uint32_t remappedCount
	) {
		this->type = CLIENT_EVENT_TYPE_DEGRADED_LOCK_RESPONSE_REMAPPED;
		this->set( instanceId, requestId );
		this->socket = socket;
		this->message.degradedLock.key = key;
		this->message.degradedLock.remappedCount = remappedCount;
		this->message.degradedLock.original = original;
		this->message.degradedLock.remapped = remapped;
	}

	inline void resDegradedLock(
		ClientSocket *socket, uint16_t instanceId, uint32_t requestId,
		Key &key, bool exist
	) {
		this->type = exist ? CLIENT_EVENT_TYPE_DEGRADED_LOCK_RESPONSE_NOT_LOCKED : CLIENT_EVENT_TYPE_DEGRADED_LOCK_RESPONSE_NOT_FOUND;
		this->set( instanceId, requestId );
		this->socket = socket;
		this->message.degradedLock.key = key;
	}

	inline void resDegradedSetLock(
		ClientSocket *socket, uint16_t instanceId, uint32_t requestId, bool success,
		uint32_t *original, uint32_t *remapped, uint32_t remappedCount, Key &key
	) {
		this->type = success ? CLIENT_EVENT_TYPE_DEGRADED_SET_LOCK_RESPONSE_SUCCESS : CLIENT_EVENT_TYPE_DEGRADED_SET_LOCK_RESPONSE_FAILURE;
		this->set( instanceId, requestId );
		this->socket = socket;
		this->message.remap = {
			.original = original,
			.remapped = remapped,
			.remappedCount = remappedCount,
			.key = key
		};
	}

	inline void announceServerReconstructed( ServerSocket *srcSocket, ServerSocket *dstSocket ) {
		this->type = CLIENT_EVENT_TYPE_ANNOUNCE_SERVER_RECONSTRUCTED;
		this->message.reconstructed = {
			.src = srcSocket,
			.dst = dstSocket
		};
	}

	// Pending
	inline void pending( ClientSocket *socket ) {
		this->type = CLIENT_EVENT_TYPE_PENDING;
		this->socket = socket;
	}
};

#endif

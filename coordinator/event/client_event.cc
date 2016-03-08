#include <vector>
#include "client_event.hh"

void ClientEvent::resRegister( ClientSocket *socket, uint16_t instanceId, uint32_t requestId, bool success ) {
	this->type = success ? CLIENT_EVENT_TYPE_REGISTER_RESPONSE_SUCCESS : CLIENT_EVENT_TYPE_REGISTER_RESPONSE_FAILURE;
	this->instanceId = instanceId;
	this->requestId = requestId;
	this->socket = socket;
}

void ClientEvent::reqPushLoadStats( ClientSocket *socket, ArrayMap<struct sockaddr_in, Latency> *slaveGetLatency,
		ArrayMap<struct sockaddr_in, Latency> *slaveSetLatency, std::set<struct sockaddr_in> *overloadedServerSet ) {
	this->type = CLIENT_EVENT_TYPE_PUSH_LOADING_STATS;
	this->socket = socket;
	this->message.slaveLoading.slaveGetLatency = slaveGetLatency;
	this->message.slaveLoading.slaveSetLatency = slaveSetLatency;
	this->message.slaveLoading.overloadedServerSet = overloadedServerSet;
}

void ClientEvent::resRemappingSetLock( ClientSocket *socket, uint16_t instanceId, uint32_t requestId, bool success, uint32_t *original, uint32_t *remapped, uint32_t remappedCount, Key &key ) {
	this->type = success ? CLIENT_EVENT_TYPE_REMAPPING_SET_LOCK_RESPONSE_SUCCESS : CLIENT_EVENT_TYPE_REMAPPING_SET_LOCK_RESPONSE_FAILURE;
	this->instanceId = instanceId;
	this->requestId = requestId;
	this->socket = socket;
	this->message.remap.original = original;
	this->message.remap.remapped = remapped;
	this->message.remap.remappedCount = remappedCount;
	this->message.remap.key = key;
}

void ClientEvent::switchPhase( bool toRemap, std::set<struct sockaddr_in> slaves, bool isCrashed, bool forced ) {
	this->type = CLIENT_EVENT_TYPE_SWITCH_PHASE;
	this->message.switchPhase.toRemap = toRemap;
	this->message.switchPhase.isCrashed = isCrashed;
	this->message.switchPhase.slaves = new std::vector<struct sockaddr_in>( slaves.begin(), slaves.end() );
	this->message.switchPhase.forced = forced;
}

void ClientEvent::resDegradedLock(
	ClientSocket *socket, uint16_t instanceId, uint32_t requestId,
	Key &key, bool isLocked, bool isSealed,
	uint32_t stripeId, uint32_t dataChunkId, uint32_t dataChunkCount,
	uint32_t *original, uint32_t *reconstructed, uint32_t reconstructedCount,
	uint32_t ongoingAtChunk, uint8_t numSurvivingChunkIds, uint32_t *survivingChunkIds
) {
	this->type = isLocked ? CLIENT_EVENT_TYPE_DEGRADED_LOCK_RESPONSE_IS_LOCKED : CLIENT_EVENT_TYPE_DEGRADED_LOCK_RESPONSE_WAS_LOCKED;
	this->instanceId = instanceId;
	this->requestId = requestId;
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
}

void ClientEvent::resDegradedLock( ClientSocket *socket, uint16_t instanceId, uint32_t requestId, Key &key, bool exist ) {
	this->type = exist ? CLIENT_EVENT_TYPE_DEGRADED_LOCK_RESPONSE_NOT_LOCKED : CLIENT_EVENT_TYPE_DEGRADED_LOCK_RESPONSE_NOT_FOUND;
	this->instanceId = instanceId;
	this->requestId = requestId;
	this->socket = socket;
	this->message.degradedLock.key = key;
}

void ClientEvent::resDegradedLock( ClientSocket *socket, uint16_t instanceId, uint32_t requestId, Key &key, uint32_t *original, uint32_t *remapped, uint32_t remappedCount ) {
	this->type = CLIENT_EVENT_TYPE_DEGRADED_LOCK_RESPONSE_REMAPPED;
	this->instanceId = instanceId;
	this->requestId = requestId;
	this->socket = socket;
	this->message.degradedLock.key = key;
	this->message.degradedLock.remappedCount = remappedCount;
	this->message.degradedLock.original = original;
	this->message.degradedLock.remapped = remapped;
}

void ClientEvent::announceServerReconstructed( ServerSocket *srcSocket, ServerSocket *dstSocket ) {
	this->type = CLIENT_EVENT_TYPE_ANNOUNCE_SERVER_RECONSTRUCTED;
	this->message.reconstructed.src = srcSocket;
	this->message.reconstructed.dst = dstSocket;
}

void ClientEvent::pending( ClientSocket *socket ) {
	this->type = CLIENT_EVENT_TYPE_PENDING;
	this->socket = socket;
}

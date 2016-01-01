#include <vector>
#include "master_event.hh"

void MasterEvent::resRegister( MasterSocket *socket, uint16_t instanceId, uint32_t requestId, bool success ) {
	this->type = success ? MASTER_EVENT_TYPE_REGISTER_RESPONSE_SUCCESS : MASTER_EVENT_TYPE_REGISTER_RESPONSE_FAILURE;
	this->instanceId = instanceId;
	this->requestId = requestId;
	this->socket = socket;
}

void MasterEvent::reqPushLoadStats( MasterSocket *socket, ArrayMap<struct sockaddr_in, Latency> *slaveGetLatency,
		ArrayMap<struct sockaddr_in, Latency> *slaveSetLatency, std::set<struct sockaddr_in> *overloadedSlaveSet ) {
	this->type = MASTER_EVENT_TYPE_PUSH_LOADING_STATS;
	this->socket = socket;
	this->message.slaveLoading.slaveGetLatency = slaveGetLatency;
	this->message.slaveLoading.slaveSetLatency = slaveSetLatency;
	this->message.slaveLoading.overloadedSlaveSet = overloadedSlaveSet;
}

void MasterEvent::resRemappingSetLock( MasterSocket *socket, uint16_t instanceId, uint32_t requestId, bool success, uint32_t *original, uint32_t *remapped, uint32_t remappedCount, Key &key ) {
	this->type = success ? MASTER_EVENT_TYPE_REMAPPING_SET_LOCK_RESPONSE_SUCCESS : MASTER_EVENT_TYPE_REMAPPING_SET_LOCK_RESPONSE_FAILURE;
	this->instanceId = instanceId;
	this->requestId = requestId;
	this->socket = socket;
	this->message.remap.original = original;
	this->message.remap.remapped = remapped;
	this->message.remap.remappedCount = remappedCount;
	this->message.remap.key = key;
}

void MasterEvent::switchPhase( bool toRemap, std::set<struct sockaddr_in> slaves ) {
	this->type = MASTER_EVENT_TYPE_SWITCH_PHASE;
	this->message.switchPhase.toRemap = toRemap;
	this->message.switchPhase.slaves = new std::vector<struct sockaddr_in>( slaves.begin(), slaves.end() );
}

void MasterEvent::resDegradedLock( MasterSocket *socket, uint16_t instanceId, uint32_t requestId, Key &key, bool isLocked, bool isSealed, uint32_t listId, uint32_t stripeId, uint32_t srcDataChunkId, uint32_t dstDataChunkId, uint32_t srcParityChunkId, uint32_t dstParityChunkId ) {
	this->type = isLocked ? MASTER_EVENT_TYPE_DEGRADED_LOCK_RESPONSE_IS_LOCKED : MASTER_EVENT_TYPE_DEGRADED_LOCK_RESPONSE_WAS_LOCKED;
	this->instanceId = instanceId;
	this->requestId = requestId;
	this->socket = socket;
	this->message.degradedLock.key = key;
	this->message.degradedLock.listId = listId;
	this->message.degradedLock.stripeId = stripeId;
	this->message.degradedLock.srcDataChunkId = srcDataChunkId;
	this->message.degradedLock.dstDataChunkId = dstDataChunkId;
	this->message.degradedLock.srcParityChunkId = srcParityChunkId;
	this->message.degradedLock.dstParityChunkId = dstParityChunkId;
	this->message.degradedLock.isSealed = isSealed;
}

void MasterEvent::resDegradedLock( MasterSocket *socket, uint16_t instanceId, uint32_t requestId, Key &key, bool exist, uint32_t listId, uint32_t srcDataChunkId, uint32_t srcParityChunkId ) {
	this->type = exist ? MASTER_EVENT_TYPE_DEGRADED_LOCK_RESPONSE_NOT_LOCKED : MASTER_EVENT_TYPE_DEGRADED_LOCK_RESPONSE_NOT_FOUND;
	this->instanceId = instanceId;
	this->requestId = requestId;
	this->socket = socket;
	this->message.degradedLock.key = key;
	this->message.degradedLock.listId = listId;
	this->message.degradedLock.srcDataChunkId = srcDataChunkId;
	this->message.degradedLock.srcParityChunkId = srcParityChunkId;
}

void MasterEvent::resDegradedLock( MasterSocket *socket, uint16_t instanceId, uint32_t requestId, Key &key, uint32_t listId, uint32_t srcDataChunkId, uint32_t dstDataChunkId, uint32_t srcParityChunkId, uint32_t dstParityChunkId ) {
	this->type = MASTER_EVENT_TYPE_DEGRADED_LOCK_RESPONSE_REMAPPED;
	this->instanceId = instanceId;
	this->requestId = requestId;
	this->socket = socket;
	this->message.degradedLock.key = key;
	this->message.degradedLock.listId = listId;
	this->message.degradedLock.srcDataChunkId = srcDataChunkId;
	this->message.degradedLock.dstDataChunkId = dstDataChunkId;
	this->message.degradedLock.srcParityChunkId = srcParityChunkId;
	this->message.degradedLock.dstParityChunkId = dstParityChunkId;
}

void MasterEvent::pending( MasterSocket *socket ) {
	this->type = MASTER_EVENT_TYPE_PENDING;
	this->socket = socket;
}

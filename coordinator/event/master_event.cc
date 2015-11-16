#include <vector>
#include "master_event.hh"

void MasterEvent::resRegister( MasterSocket *socket, uint32_t id, bool success ) {
	this->type = success ? MASTER_EVENT_TYPE_REGISTER_RESPONSE_SUCCESS : MASTER_EVENT_TYPE_REGISTER_RESPONSE_FAILURE;
	this->id = id;
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

void MasterEvent::resRemappingSetLock( MasterSocket *socket, uint32_t id, bool isRemapped, Key &key, RemappingRecord &remappingRecord, bool success ) {
	this->type = success ? MASTER_EVENT_TYPE_REMAPPING_SET_LOCK_RESPONSE_SUCCESS : MASTER_EVENT_TYPE_REMAPPING_SET_LOCK_RESPONSE_FAILURE;
	this->id = id;
	this->socket = socket;
	this->message.remap.key = key;
	this->message.remap.listId = remappingRecord.listId;
	this->message.remap.chunkId = remappingRecord.chunkId;
	this->message.remap.isRemapped = isRemapped;
}

void MasterEvent::switchPhase( bool toRemap, std::set<struct sockaddr_in> slaves ) {
	this->type = MASTER_EVENT_TYPE_SWITCH_PHASE;
	this->message.remap.toRemap = toRemap;
	this->message.remap.slaves = new std::vector<struct sockaddr_in>( slaves.begin(), slaves.end() );
}

void MasterEvent::syncRemappingRecords( MasterSocket *socket, std::vector<Packet*> *packets ) {
	this->type = MASTER_EVENT_TYPE_SYNC_REMAPPING_RECORDS;
	this->socket = socket;
	this->message.remap.syncPackets = packets;
}

void MasterEvent::resDegradedLock( MasterSocket *socket, uint32_t id, Key &key, bool isLocked, bool isSealed, uint32_t srcListId, uint32_t srcStripeId, uint32_t srcChunkId, uint32_t dstListId, uint32_t dstChunkId ) {
	this->type = isLocked ? MASTER_EVENT_TYPE_DEGRADED_LOCK_RESPONSE_IS_LOCKED : MASTER_EVENT_TYPE_DEGRADED_LOCK_RESPONSE_WAS_LOCKED;
	this->id = id;
	this->socket = socket;
	this->message.degradedLock.key = key;
	this->message.degradedLock.srcListId = srcListId;
	this->message.degradedLock.srcStripeId = srcStripeId;
	this->message.degradedLock.srcChunkId = srcChunkId;
	this->message.degradedLock.dstListId = dstListId;
	this->message.degradedLock.dstChunkId = dstChunkId;
	this->message.degradedLock.isSealed = isSealed;
}

void MasterEvent::resDegradedLock( MasterSocket *socket, uint32_t id, Key &key, bool isRemapped, uint32_t listId, uint32_t chunkId ) {
	this->type = isRemapped ? MASTER_EVENT_TYPE_DEGRADED_LOCK_RESPONSE_REMAPPED : MASTER_EVENT_TYPE_DEGRADED_LOCK_RESPONSE_NOT_LOCKED;
	this->id = id;
	this->socket = socket;
	this->message.degradedLock.key = key;
	this->message.degradedLock.srcListId = listId;
	this->message.degradedLock.srcChunkId = chunkId;
}

void MasterEvent::resDegradedLock( MasterSocket *socket, uint32_t id, Key &key ) {
	this->type = MASTER_EVENT_TYPE_DEGRADED_LOCK_RESPONSE_NOT_FOUND;
	this->id = id;
	this->socket = socket;
	this->message.degradedLock.key = key;
}

void MasterEvent::pending( MasterSocket *socket ) {
	this->type = MASTER_EVENT_TYPE_PENDING;
	this->socket = socket;
}

#include "master_event.hh"

void MasterEvent::resRegister( MasterSocket *socket, uint32_t id, bool success ) {
	this->type = success ? MASTER_EVENT_TYPE_REGISTER_RESPONSE_SUCCESS : MASTER_EVENT_TYPE_REGISTER_RESPONSE_FAILURE;
	this->id = id;
	this->socket = socket;
}

void MasterEvent::resGet( MasterSocket *socket, uint32_t id, KeyValue &keyValue ) {
	this->type = MASTER_EVENT_TYPE_GET_RESPONSE_SUCCESS;
	this->id = id;
	this->socket = socket;
	this->message.keyValue = keyValue;
}

void MasterEvent::resGet( MasterSocket *socket, uint32_t id, Key &key ) {
	this->type = MASTER_EVENT_TYPE_GET_RESPONSE_FAILURE;
	this->id = id;
	this->socket = socket;
	this->message.key = key;
}

void MasterEvent::resSet( MasterSocket *socket, uint32_t id, Key &key, bool success ) {
	this->type = success ? MASTER_EVENT_TYPE_SET_RESPONSE_SUCCESS : MASTER_EVENT_TYPE_SET_RESPONSE_FAILURE;
	this->id = id;
	this->socket = socket;
	this->message.key = key;
}

void MasterEvent::resRemappingSetLock( MasterSocket *socket, uint32_t id, Key &key, RemappingRecord &remappingRecord, bool success ) {
	this->type = success ? MASTER_EVENT_TYPE_REMAPPING_SET_LOCK_RESPONSE_SUCCESS : MASTER_EVENT_TYPE_REMAPPING_SET_LOCK_RESPONSE_FAILURE;
	this->id = id;
	this->socket = socket;
	this->message.remap.key = key;
	this->message.remap.listId = remappingRecord.listId;
	this->message.remap.chunkId = remappingRecord.chunkId;
}

void MasterEvent::resRemappingSet( MasterSocket *socket, uint32_t id, Key &key, uint32_t listId, uint32_t chunkId, bool success, bool needsFree ) {
	this->type = success ? MASTER_EVENT_TYPE_REMAPPING_SET_RESPONSE_SUCCESS : MASTER_EVENT_TYPE_REMAPPING_SET_RESPONSE_FAILURE;
	this->id = id;
	this->socket = socket;
	this->message.remap.key = key;
	this->message.remap.listId = listId;
	this->message.remap.chunkId = chunkId;
	this->needsFree = needsFree;
}

void MasterEvent::resUpdate( MasterSocket *socket, uint32_t id, Key &key, uint32_t valueUpdateOffset, uint32_t valueUpdateSize, bool success, bool needsFree ) {
	this->type = success ? MASTER_EVENT_TYPE_UPDATE_RESPONSE_SUCCESS : MASTER_EVENT_TYPE_UPDATE_RESPONSE_FAILURE;
	this->id = id;
	this->socket = socket;
	this->message.keyValueUpdate.key = key;
	this->message.keyValueUpdate.valueUpdateOffset = valueUpdateOffset;
	this->message.keyValueUpdate.valueUpdateSize = valueUpdateSize;
	this->needsFree = needsFree;
}

void MasterEvent::resDelete( MasterSocket *socket, uint32_t id, Key &key, bool success, bool needsFree ) {
	this->type = success ? MASTER_EVENT_TYPE_DELETE_RESPONSE_SUCCESS : MASTER_EVENT_TYPE_DELETE_RESPONSE_FAILURE;
	this->id = id;
	this->socket = socket;
	this->message.key = key;
	this->needsFree = needsFree;
}

void MasterEvent::resRedirect( MasterSocket *socket, uint32_t id, uint8_t opcode, Key &key, RemappingRecord record ) {
	this->type = MASTER_EVENT_TYPE_REDIRECT_RESPONSE;
	this->id = id;
	this->needsFree = false;
	this->socket = socket;
	this->message.remap.key = key;
	this->message.remap.opcode = opcode;
	this->message.remap.listId = record.listId;
	this->message.remap.chunkId = record.chunkId;
}

void MasterEvent::resDegradedLock( MasterSocket *socket, uint32_t id, Key &key, bool isLocked, uint32_t listId, uint32_t stripeId, uint32_t chunkId ) {
	this->type = isLocked ? MASTER_EVENT_TYPE_DEGRADED_LOCK_RESPONSE_IS_LOCKED : MASTER_EVENT_TYPE_DEGRADED_LOCK_RESPONSE_WAS_LOCKED;
	this->id = id;
	this->needsFree = false;
	this->socket = socket;
	this->message.degradedLock.key = key;
	this->message.degradedLock.listId = listId;
	this->message.degradedLock.stripeId = stripeId;
	this->message.degradedLock.chunkId = chunkId;
}

void MasterEvent::resDegradedLock( MasterSocket *socket, uint32_t id, Key &key, uint32_t listId, uint32_t chunkId ) {
	this->type = MASTER_EVENT_TYPE_DEGRADED_LOCK_RESPONSE_REMAPPED;
	this->id = id;
	this->needsFree = false;
	this->socket = socket;
	this->message.degradedLock.key = key;
	this->message.degradedLock.listId = listId;
	this->message.degradedLock.chunkId = chunkId;
}

void MasterEvent::resDegradedLock( MasterSocket *socket, uint32_t id, Key &key ) {
	this->type = MASTER_EVENT_TYPE_DEGRADED_LOCK_RESPONSE_NOT_FOUND;
	this->id = id;
	this->needsFree = false;
	this->socket = socket;
	this->message.degradedLock.key = key;
}

void MasterEvent::pending( MasterSocket *socket ) {
	this->type = MASTER_EVENT_TYPE_PENDING;
	this->socket = socket;
}

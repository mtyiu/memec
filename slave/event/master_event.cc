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

void MasterEvent::pending( MasterSocket *socket ) {
	this->type = MASTER_EVENT_TYPE_PENDING;
	this->socket = socket;
}

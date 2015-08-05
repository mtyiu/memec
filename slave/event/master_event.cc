#include "master_event.hh"

void MasterEvent::resRegister( MasterSocket *socket, bool success ) {
	this->type = success ? MASTER_EVENT_TYPE_REGISTER_RESPONSE_SUCCESS : MASTER_EVENT_TYPE_REGISTER_RESPONSE_FAILURE;
	this->socket = socket;
}

void MasterEvent::resGet( MasterSocket *socket, KeyValue &keyValue ) {
	this->type = MASTER_EVENT_TYPE_GET_RESPONSE_SUCCESS;
	this->socket = socket;
	this->message.keyValue = keyValue;
}

void MasterEvent::resGet( MasterSocket *socket, Key &key ) {
	this->type = MASTER_EVENT_TYPE_GET_RESPONSE_FAILURE;
	this->socket = socket;
	this->message.key = key;
}

void MasterEvent::resSet( MasterSocket *socket, Key &key ) {
	this->type = MASTER_EVENT_TYPE_SET_RESPONSE_SUCCESS;
	this->socket = socket;
	this->message.key = key;
}

void MasterEvent::resUpdate( MasterSocket *socket, Key &key, Metadata &metadata, uint32_t offset, uint32_t length, char *delta ) {
	this->type = MASTER_EVENT_TYPE_UPDATE_RESPONSE_SUCCESS;
	this->socket = socket;
	this->message.keyValueUpdate.key = key;
	this->message.keyValueUpdate.metadata = metadata;
	this->message.keyValueUpdate.offset = offset;
	this->message.keyValueUpdate.length = length;
	this->message.keyValueUpdate.delta = delta;
}

void MasterEvent::resUpdate( MasterSocket *socket, Key &key ) {
	this->type = MASTER_EVENT_TYPE_UPDATE_RESPONSE_FAILURE;
	this->socket = socket;
	this->message.key = key;
}

void MasterEvent::resUpdateChunk( MasterSocket *socket, Metadata &metadata, uint32_t offset, uint32_t length, bool success ) {
	this->type = success ? MASTER_EVENT_TYPE_UPDATE_CHUNK_RESPONSE_SUCCESS : MASTER_EVENT_TYPE_UPDATE_CHUNK_RESPONSE_FAILURE;
	this->socket = socket;
	this->message.chunkUpdate.metadata = metadata;
	this->message.chunkUpdate.offset = offset;
	this->message.chunkUpdate.length = length;
}

void MasterEvent::resDelete( MasterSocket *socket, Key &key, Metadata &metadata, uint32_t offset, uint32_t length, char *delta ) {
	this->type = MASTER_EVENT_TYPE_DELETE_RESPONSE_SUCCESS;
	this->socket = socket;
	this->message.keyValueUpdate.key = key;
	this->message.keyValueUpdate.metadata = metadata;
	this->message.keyValueUpdate.offset = offset;
	this->message.keyValueUpdate.length = length;
	this->message.keyValueUpdate.delta = delta;
}

void MasterEvent::resDelete( MasterSocket *socket, Key &key ) {
	this->type = MASTER_EVENT_TYPE_DELETE_RESPONSE_FAILURE;
	this->socket = socket;
	this->message.key = key;
}

void MasterEvent::resDeleteChunk( MasterSocket *socket, Metadata &metadata, uint32_t offset, uint32_t length, bool success ) {
	this->type = success ? MASTER_EVENT_TYPE_DELETE_CHUNK_RESPONSE_SUCCESS : MASTER_EVENT_TYPE_DELETE_CHUNK_RESPONSE_FAILURE;
	this->socket = socket;
	this->message.chunkUpdate.metadata = metadata;
	this->message.chunkUpdate.offset = offset;
	this->message.chunkUpdate.length = length;
}

void MasterEvent::pending( MasterSocket *socket ) {
	this->type = MASTER_EVENT_TYPE_PENDING;
	this->socket = socket;
}

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

void MasterEvent::resUpdate( MasterSocket *socket, Key &key, uint32_t valueUpdateOffset, uint32_t valueUpdateSize, Metadata &metadata, uint32_t offset, uint32_t length, char *delta ) {
	this->type = MASTER_EVENT_TYPE_UPDATE_RESPONSE_SUCCESS;
	this->socket = socket;
	this->message.keyValueChunkUpdate.key = key;
	this->message.keyValueChunkUpdate.valueUpdateOffset = valueUpdateOffset;
	this->message.keyValueChunkUpdate.valueUpdateSize = valueUpdateSize;
	this->message.keyValueChunkUpdate.metadata = metadata;
	this->message.keyValueChunkUpdate.offset = offset;
	this->message.keyValueChunkUpdate.length = length;
	this->message.keyValueChunkUpdate.delta = delta;
}

void MasterEvent::resUpdate( MasterSocket *socket, Key &key, uint32_t valueUpdateOffset, uint32_t valueUpdateSize ) {
	this->type = MASTER_EVENT_TYPE_UPDATE_RESPONSE_FAILURE;
	this->socket = socket;
	this->message.keyValueChunkUpdate.key = key;
	this->message.keyValueChunkUpdate.valueUpdateOffset = valueUpdateOffset;
	this->message.keyValueChunkUpdate.valueUpdateSize = valueUpdateSize;
	this->message.keyValueChunkUpdate.offset = 0;
	this->message.keyValueChunkUpdate.length = 0;
	this->message.keyValueChunkUpdate.delta = 0;
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
	this->message.keyChunkUpdate.key = key;
	this->message.keyChunkUpdate.metadata = metadata;
	this->message.keyChunkUpdate.offset = offset;
	this->message.keyChunkUpdate.length = length;
	this->message.keyChunkUpdate.delta = delta;
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

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

void MasterEvent::resSet( MasterSocket *socket, Key &key, bool success ) {
	this->type = success ? MASTER_EVENT_TYPE_SET_RESPONSE_SUCCESS : MASTER_EVENT_TYPE_SET_RESPONSE_FAILURE;
	this->socket = socket;
	this->message.key = key;
}

void MasterEvent::resUpdate( MasterSocket *socket, Key &key, uint32_t valueUpdateOffset, uint32_t valueUpdateSize, bool success ) {
	this->type = success ? MASTER_EVENT_TYPE_UPDATE_RESPONSE_SUCCESS : MASTER_EVENT_TYPE_UPDATE_RESPONSE_FAILURE;
	this->socket = socket;
	this->message.keyValueUpdate.key = key;
	this->message.keyValueUpdate.valueUpdateOffset = valueUpdateOffset;
	this->message.keyValueUpdate.valueUpdateSize = valueUpdateSize;
}

void MasterEvent::resDelete( MasterSocket *socket, Key &key, bool success ) {
	this->type = success ? MASTER_EVENT_TYPE_DELETE_RESPONSE_SUCCESS : MASTER_EVENT_TYPE_DELETE_RESPONSE_FAILURE;
	this->socket = socket;
	this->message.key = key;
}

void MasterEvent::pending( MasterSocket *socket ) {
	this->type = MASTER_EVENT_TYPE_PENDING;
	this->socket = socket;
}

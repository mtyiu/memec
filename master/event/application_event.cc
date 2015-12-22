#include "application_event.hh"

void ApplicationEvent::resRegister( ApplicationSocket *socket, uint32_t id, bool success ) {
	this->type = success ? APPLICATION_EVENT_TYPE_REGISTER_RESPONSE_SUCCESS : APPLICATION_EVENT_TYPE_REGISTER_RESPONSE_FAILURE;
	this->id = id;
	this->socket = socket;
}

void ApplicationEvent::resGet( ApplicationSocket *socket, uint32_t id, uint8_t keySize, uint32_t valueSize, char *keyStr, char *valueStr, bool needsFree ) {
	this->type = APPLICATION_EVENT_TYPE_GET_RESPONSE_SUCCESS;
	this->id = id;
	this->socket = socket;
	this->message.get.keySize = keySize;
	this->message.get.valueSize = valueSize;
	this->message.get.keyStr = keyStr;
	this->message.get.valueStr = valueStr;
	this->needsFree = needsFree;
}

void ApplicationEvent::resGet( ApplicationSocket *socket, uint32_t id, Key &key, bool needsFree ) {
	this->type = APPLICATION_EVENT_TYPE_GET_RESPONSE_FAILURE;
	this->id = id;
	this->socket = socket;
	this->message.key = key;
	this->needsFree = needsFree;
}

void ApplicationEvent::resSet( ApplicationSocket *socket, uint32_t id, KeyValue &keyValue, bool success, bool needsFree ) {
	this->type = success ? APPLICATION_EVENT_TYPE_SET_RESPONSE_SUCCESS : APPLICATION_EVENT_TYPE_SET_RESPONSE_FAILURE;
	this->id = id;
	this->socket = socket;
	this->message.set.isKeyValue = true;
	this->message.set.data.keyValue = keyValue;
	this->needsFree = needsFree;
}

void ApplicationEvent::resSet( ApplicationSocket *socket, uint32_t id, Key &key, bool success, bool needsFree ) {
	this->type = success ? APPLICATION_EVENT_TYPE_SET_RESPONSE_SUCCESS : APPLICATION_EVENT_TYPE_SET_RESPONSE_FAILURE;
	this->id = id;
	this->socket = socket;
	this->message.set.isKeyValue = false;
	this->message.set.data.key = key;
	this->needsFree = needsFree;
}

void ApplicationEvent::resUpdate( ApplicationSocket *socket, uint32_t id, KeyValueUpdate &keyValueUpdate, bool success, bool needsFree ) {
	this->type = success ? APPLICATION_EVENT_TYPE_UPDATE_RESPONSE_SUCCESS : APPLICATION_EVENT_TYPE_UPDATE_RESPONSE_FAILURE;
	this->id = id;
	this->socket = socket;
	this->message.keyValueUpdate = keyValueUpdate;
	this->needsFree = needsFree;
}

void ApplicationEvent::resDelete( ApplicationSocket *socket, uint32_t id, Key &key, bool success, bool needsFree ) {
	this->type = success ? APPLICATION_EVENT_TYPE_DELETE_RESPONSE_SUCCESS : APPLICATION_EVENT_TYPE_DELETE_RESPONSE_FAILURE;
	this->id = id;
	this->socket = socket;
	this->message.key = key;
	this->needsFree = needsFree;
}

void ApplicationEvent::pending( ApplicationSocket *socket ) {
	this->type = APPLICATION_EVENT_TYPE_PENDING;
	this->socket = socket;
}

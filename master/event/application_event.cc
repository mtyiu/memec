#include "application_event.hh"

void ApplicationEvent::resRegister( ApplicationSocket *socket, uint16_t instanceId, uint32_t requestId, bool success ) {
	this->type = success ? APPLICATION_EVENT_TYPE_REGISTER_RESPONSE_SUCCESS : APPLICATION_EVENT_TYPE_REGISTER_RESPONSE_FAILURE;
	this->instanceId = instanceId;
	this->requestId = requestId;
	this->socket = socket;
}

void ApplicationEvent::resGet( ApplicationSocket *socket, uint16_t instanceId, uint32_t requestId, uint8_t keySize, uint32_t valueSize, char *keyStr, char *valueStr, bool needsFree ) {
	this->type = APPLICATION_EVENT_TYPE_GET_RESPONSE_SUCCESS;
	this->instanceId = instanceId;
	this->requestId = requestId;
	this->socket = socket;
	this->message.get.keySize = keySize;
	this->message.get.valueSize = valueSize;
	this->message.get.keyStr = keyStr;
	this->message.get.valueStr = valueStr;
	this->needsFree = needsFree;
}

void ApplicationEvent::resGet( ApplicationSocket *socket, uint16_t instanceId, uint32_t requestId, Key &key, bool needsFree ) {
	this->type = APPLICATION_EVENT_TYPE_GET_RESPONSE_FAILURE;
	this->instanceId = instanceId;
	this->requestId = requestId;
	this->socket = socket;
	this->message.key = key;
	this->needsFree = needsFree;
}

void ApplicationEvent::resSet( ApplicationSocket *socket, uint16_t instanceId, uint32_t requestId, KeyValue &keyValue, bool success, bool needsFree ) {
	this->type = success ? APPLICATION_EVENT_TYPE_SET_RESPONSE_SUCCESS : APPLICATION_EVENT_TYPE_SET_RESPONSE_FAILURE;
	this->instanceId = instanceId;
	this->requestId = requestId;
	this->socket = socket;
	this->message.set.isKeyValue = true;
	this->message.set.data.keyValue = keyValue;
	this->needsFree = needsFree;
}

void ApplicationEvent::resSet( ApplicationSocket *socket, uint16_t instanceId, uint32_t requestId, Key &key, bool success, bool needsFree ) {
	this->type = success ? APPLICATION_EVENT_TYPE_SET_RESPONSE_SUCCESS : APPLICATION_EVENT_TYPE_SET_RESPONSE_FAILURE;
	this->instanceId = instanceId;
	this->requestId = requestId;
	this->socket = socket;
	this->message.set.isKeyValue = false;
	this->message.set.data.key = key;
	this->needsFree = needsFree;
}

void ApplicationEvent::resUpdate( ApplicationSocket *socket, uint16_t instanceId, uint32_t requestId, KeyValueUpdate &keyValueUpdate, bool success, bool needsFree ) {
	this->type = success ? APPLICATION_EVENT_TYPE_UPDATE_RESPONSE_SUCCESS : APPLICATION_EVENT_TYPE_UPDATE_RESPONSE_FAILURE;
	this->instanceId = instanceId;
	this->requestId = requestId;
	this->socket = socket; this->message.keyValueUpdate = keyValueUpdate;
	this->needsFree = needsFree;
}

void ApplicationEvent::resDelete( ApplicationSocket *socket, uint16_t instanceId, uint32_t requestId, Key &key, bool success, bool needsFree ) {
	this->type = success ? APPLICATION_EVENT_TYPE_DELETE_RESPONSE_SUCCESS : APPLICATION_EVENT_TYPE_DELETE_RESPONSE_FAILURE;
	this->instanceId = instanceId;
	this->requestId = requestId;
	this->socket = socket;
	this->message.key = key;
	this->needsFree = needsFree;
}

void ApplicationEvent::replaySetRequest( ApplicationSocket *socket, uint16_t instanceId, uint32_t requestId, KeyValue keyValue ) {
	this->type = APPLICATION_EVENT_TYPE_REPLAY_SET;
	this->socket = socket;
	this->instanceId = instanceId;
	this->requestId = requestId;
	this->message.replay.set.keyValue= keyValue;
}

void ApplicationEvent::replayGetRequest( ApplicationSocket *socket, uint16_t instanceId, uint32_t requestId, Key key ) {
	this->type = APPLICATION_EVENT_TYPE_REPLAY_GET;
	this->socket = socket;
	this->instanceId = instanceId;
	this->requestId = requestId;
	this->message.replay.get.key = key;
}

void ApplicationEvent::replayUpdateRequest( ApplicationSocket *socket, uint16_t instanceId, uint32_t requestId, KeyValueUpdate keyValueUpdate ) {
	this->type = APPLICATION_EVENT_TYPE_REPLAY_UPDATE;
	this->socket = socket;
	this->instanceId = instanceId;
	this->requestId = requestId;
	this->message.replay.update.keyValueUpdate = keyValueUpdate;
}

void ApplicationEvent::replayDeleteRequest( ApplicationSocket *socket, uint16_t instanceId, uint32_t requestId, Key key ) {
	this->type = APPLICATION_EVENT_TYPE_REPLAY_DEL;
	this->socket = socket;
	this->instanceId = instanceId;
	this->requestId = requestId;
	this->message.replay.del.key = key;
}

void ApplicationEvent::pending( ApplicationSocket *socket ) {
	this->type = APPLICATION_EVENT_TYPE_PENDING;
	this->socket = socket;
}

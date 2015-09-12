#include "application_event.hh"

void ApplicationEvent::resRegister( ApplicationSocket *socket, bool success ) {
	this->type = success ? APPLICATION_EVENT_TYPE_REGISTER_RESPONSE_SUCCESS : APPLICATION_EVENT_TYPE_REGISTER_RESPONSE_FAILURE;
	this->socket = socket;
}

void ApplicationEvent::resGet( ApplicationSocket *socket, KeyValue &keyValue, bool needsFree ) {
	this->type = APPLICATION_EVENT_TYPE_GET_RESPONSE_SUCCESS;
	this->socket = socket;
	this->message.keyValue = keyValue;
	this->needsFree = needsFree;
}

void ApplicationEvent::resGet( ApplicationSocket *socket, Key &key, bool needsFree ) {
	this->type = APPLICATION_EVENT_TYPE_GET_RESPONSE_FAILURE;
	this->socket = socket;
	this->message.key = key;
	this->needsFree = needsFree;
}

void ApplicationEvent::resSet( ApplicationSocket *socket, Key &key, bool success, bool needsFree ) {
	this->type = success ? APPLICATION_EVENT_TYPE_SET_RESPONSE_SUCCESS : APPLICATION_EVENT_TYPE_SET_RESPONSE_FAILURE;
	this->socket = socket;
	this->message.key = key;
	this->needsFree = needsFree;
}

void ApplicationEvent::resUpdate( ApplicationSocket *socket, KeyValueUpdate &keyValueUpdate, bool success, bool needsFree ) {
	this->type = success ? APPLICATION_EVENT_TYPE_UPDATE_RESPONSE_SUCCESS : APPLICATION_EVENT_TYPE_UPDATE_RESPONSE_FAILURE;
	this->socket = socket;
	this->message.keyValueUpdate = keyValueUpdate;
	this->needsFree = needsFree;
}

void ApplicationEvent::resDelete( ApplicationSocket *socket, Key &key, bool success, bool needsFree ) {
	this->type = success ? APPLICATION_EVENT_TYPE_DELETE_RESPONSE_SUCCESS : APPLICATION_EVENT_TYPE_DELETE_RESPONSE_FAILURE;
	this->socket = socket;
	this->message.key = key;
	this->needsFree = needsFree;
}

void ApplicationEvent::pending( ApplicationSocket *socket ) {
	this->type = APPLICATION_EVENT_TYPE_PENDING;
	this->socket = socket;
}

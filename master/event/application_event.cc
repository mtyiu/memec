#include "application_event.hh"

void ApplicationEvent::resRegister( ApplicationSocket *socket, bool success ) {
	this->type = success ? APPLICATION_EVENT_TYPE_REGISTER_RESPONSE_SUCCESS : APPLICATION_EVENT_TYPE_REGISTER_RESPONSE_FAILURE;
	this->socket = socket;
}

void ApplicationEvent::resGet( ApplicationSocket *socket, KeyValue &keyValue ) {
	this->type = APPLICATION_EVENT_TYPE_GET_RESPONSE_SUCCESS;
	this->socket = socket;
	this->message.keyValue = keyValue;
}

void ApplicationEvent::resGet( ApplicationSocket *socket, Key &key ) {
	this->type = APPLICATION_EVENT_TYPE_GET_RESPONSE_FAILURE;
	this->socket = socket;
	this->message.key = key;
}

void ApplicationEvent::resSet( ApplicationSocket *socket, Key &key, bool success ) {
	this->type = success ? APPLICATION_EVENT_TYPE_SET_RESPONSE_SUCCESS : APPLICATION_EVENT_TYPE_SET_RESPONSE_FAILURE;
	this->socket = socket;
	this->message.key = key;
}

void ApplicationEvent::pending( ApplicationSocket *socket ) {
	this->type = APPLICATION_EVENT_TYPE_PENDING;
	this->socket = socket;
}

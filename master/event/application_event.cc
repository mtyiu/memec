#include "application_event.hh"

void ApplicationEvent::resRegister( ApplicationSocket *socket, bool success ) {
	this->type = success ? APPLICATION_EVENT_TYPE_REGISTER_RESPONSE_SUCCESS : APPLICATION_EVENT_TYPE_REGISTER_RESPONSE_FAILURE;
	this->socket = socket;
}

void ApplicationEvent::pending( ApplicationSocket *socket ) {
	this->type = APPLICATION_EVENT_TYPE_PENDING;
	this->socket = socket;
}

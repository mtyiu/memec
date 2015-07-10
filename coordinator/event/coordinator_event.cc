#include "coordinator_event.hh"

void CoordinatorEvent::reqRegister( CoordinatorSocket *socket ) {
	this->type = COORDINATOR_EVENT_TYPE_REGISTER_REQUEST;
	this->socket = socket;
}

void CoordinatorEvent::resRegister( CoordinatorSocket *socket, bool success ) {
	this->type = success ? COORDINATOR_EVENT_TYPE_REGISTER_RESPONSE_SUCCESS : COORDINATOR_EVENT_TYPE_REGISTER_RESPONSE_FAILURE;
	this->socket = socket;
}

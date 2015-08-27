#include "coordinator_event.hh"

void CoordinatorEvent::reqRegister( CoordinatorSocket *socket, uint32_t addr, uint16_t port ) {
	this->type = COORDINATOR_EVENT_TYPE_REGISTER_REQUEST;
	this->socket = socket;
	this->message.address.addr = addr;
	this->message.address.port = port;
}

void CoordinatorEvent::sync( CoordinatorSocket *socket ) {
	this->type = COORDINATOR_EVENT_TYPE_SYNC;
	this->socket = socket;
}

void CoordinatorEvent::pending( CoordinatorSocket *socket ) {
	this->type = COORDINATOR_EVENT_TYPE_PENDING;
	this->socket = socket;
}

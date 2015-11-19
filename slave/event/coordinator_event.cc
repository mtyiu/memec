#include "coordinator_event.hh"

void CoordinatorEvent::reqRegister( CoordinatorSocket *socket, uint32_t addr, uint16_t port ) {
	this->type = COORDINATOR_EVENT_TYPE_REGISTER_REQUEST;
	this->socket = socket;
	this->message.address.addr = addr;
	this->message.address.port = port;
}

void CoordinatorEvent::sync( CoordinatorSocket *socket, uint32_t id ) {
	this->type = COORDINATOR_EVENT_TYPE_SYNC;
	this->socket = socket;
	this->id = id;
}

void CoordinatorEvent::syncRemap( CoordinatorSocket *socket ) {
	this->type = COORDINATOR_EVENT_TYPE_REMAP_SYNC;
	this->socket = socket;
}

void CoordinatorEvent::resReleaseDegradedLock( CoordinatorSocket *socket, uint32_t id, uint32_t count ) {
	this->type = COORDINATOR_EVENT_TYPE_RELEASE_DEGRADED_LOCK;
	this->socket = socket;
	this->id = id;
	this->message.degraded.count = count;
}

void CoordinatorEvent::pending( CoordinatorSocket *socket ) {
	this->type = COORDINATOR_EVENT_TYPE_PENDING;
	this->socket = socket;
}

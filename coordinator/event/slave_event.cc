#include "slave_event.hh"

void SlaveEvent::pending( SlaveSocket *socket ) {
	this->type = SLAVE_EVENT_TYPE_PENDING;
	this->socket = socket;
}

void SlaveEvent::resRegister( SlaveSocket *socket, bool success ) {
	this->type = success ? SLAVE_EVENT_TYPE_REGISTER_RESPONSE_SUCCESS : SLAVE_EVENT_TYPE_REGISTER_RESPONSE_FAILURE;
	this->socket = socket;
}

void SlaveEvent::announceSlaveConnected( SlaveSocket *socket ) {
	this->type = SLAVE_EVENT_TYPE_ANNOUNCE_SLAVE_CONNECTED;
	this->socket = socket;
}

#include "slave_event.hh"

void SlaveEvent::reqRegister( SlaveSocket *socket ) {
	this->type = SLAVE_EVENT_TYPE_REGISTER_REQUEST;
	this->socket = socket;
}

void SlaveEvent::pending( SlaveSocket *socket ) {
	this->type = SLAVE_EVENT_TYPE_PENDING;
	this->socket = socket;
}

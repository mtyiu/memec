#include "server_event.hh"

void SlaveEvent::pending( SlaveSocket *socket ) {
	this->type = SLAVE_EVENT_TYPE_PENDING;
	this->socket = socket;
}

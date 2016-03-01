#include "server_event.hh"

void SlaveEvent::pending( ServerSocket *socket ) {
	this->type = SLAVE_EVENT_TYPE_PENDING;
	this->socket = socket;
}

#include "client_event.hh"

void MasterEvent::pending( ClientSocket *socket ) {
	this->type = MASTER_EVENT_TYPE_PENDING;
	this->socket = socket;
}

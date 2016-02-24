#include "master_event.hh"

void MasterEvent::pending( MasterSocket *socket ) {
	this->type = MASTER_EVENT_TYPE_PENDING;
	this->socket = socket;
}

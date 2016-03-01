#include "client_event.hh"

void ClientEvent::pending( ClientSocket *socket ) {
	this->type = CLIENT_EVENT_TYPE_PENDING;
	this->socket = socket;
}

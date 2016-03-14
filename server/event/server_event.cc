#include "server_event.hh"

void ServerEvent::pending( ServerSocket *socket ) {
	this->type = SERVER_EVENT_TYPE_PENDING;
	this->socket = socket;
}

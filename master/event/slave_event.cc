#include "slave_event.hh"

void SlaveEvent::reqRegister( SlaveSocket *socket ) {
	this->type = SLAVE_EVENT_TYPE_REGISTER_REQUEST;
	this->socket = socket;
}

void SlaveEvent::send( SlaveSocket *socket, MasterProtocol *protocol, size_t size, size_t index ) {
	this->type = SLAVE_EVENT_TYPE_SEND;
	this->socket = socket;
	this->message.send.protocol = protocol;
	this->message.send.size = size;
	this->message.send.index = index;
}

void SlaveEvent::pending( SlaveSocket *socket ) {
	this->type = SLAVE_EVENT_TYPE_PENDING;
	this->socket = socket;
}

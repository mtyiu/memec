#include "slave_event.hh"

void SlaveEvent::reqRegister( SlaveSocket *socket, uint32_t addr, uint16_t port ) {
	this->type = SLAVE_EVENT_TYPE_REGISTER_REQUEST;
	this->socket = socket;
	this->message.address.addr = addr;
	this->message.address.port = port;
}

void SlaveEvent::send( SlaveSocket *socket, Packet *packet ) {
	this->type = SLAVE_EVENT_TYPE_SEND;
	this->socket = socket;
	this->message.send.packet = packet;
}

void SlaveEvent::pending( SlaveSocket *socket ) {
	this->type = SLAVE_EVENT_TYPE_PENDING;
	this->socket = socket;
}

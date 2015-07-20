#include "master_event.hh"

void MasterEvent::reqRegister( MasterSocket *socket ) {
	this->type = MASTER_EVENT_TYPE_REGISTER_REQUEST;
	this->socket = socket;
}

void MasterEvent::reqSet( MasterSocket *socket, char *key, char *value, size_t size ) {
	this->type = MASTER_EVENT_TYPE_SET_REQUEST;
	this->socket = socket;
	this->message.set.key = key;
	this->message.set.value = value;
	this->message.set.size = size;
}

void MasterEvent::reqGet( MasterSocket *socket, char *key, int fd ) {
	this->type = MASTER_EVENT_TYPE_GET_REQUEST;
	this->socket = socket;
	this->message.get.key = key;
	this->message.get.fd = fd;
}

void MasterEvent::pending( MasterSocket *socket ) {
	this->type = MASTER_EVENT_TYPE_PENDING;
	this->socket = socket;
}

#include "master_event.hh"

void MasterEvent::reqRegister( MasterSocket *socket ) {
	this->type = MASTER_EVENT_TYPE_REGISTER_REQUEST;
	this->socket = socket;
}

void MasterEvent::reqSet( MasterSocket *socket, char *key, uint32_t keySize, int fd ) {
	this->type = MASTER_EVENT_TYPE_SET_REQUEST;
	this->socket = socket;
	this->message.set.key = key;
	this->message.set.keySize = keySize;
	this->message.set.fd = fd;
}

void MasterEvent::reqGet( MasterSocket *socket, char *key, uint32_t keySize, int fd ) {
	this->type = MASTER_EVENT_TYPE_GET_REQUEST;
	this->socket = socket;
	this->message.get.key = key;
	this->message.set.keySize = keySize;
	this->message.get.fd = fd;
}

void MasterEvent::pending( MasterSocket *socket ) {
	this->type = MASTER_EVENT_TYPE_PENDING;
	this->socket = socket;
}

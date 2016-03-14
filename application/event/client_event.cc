#include "client_event.hh"

void ClientEvent::reqRegister( ClientSocket *socket ) {
	this->type = CLIENT_EVENT_TYPE_REGISTER_REQUEST;
	this->socket = socket;
}

void ClientEvent::reqSet( ClientSocket *socket, char *key, uint32_t keySize, int fd ) {
	this->type = CLIENT_EVENT_TYPE_SET_REQUEST;
	this->socket = socket;
	this->message.set.key = key;
	this->message.set.keySize = keySize;
	this->message.set.fd = fd;
}

void ClientEvent::reqGet( ClientSocket *socket, char *key, uint32_t keySize, int fd ) {
	this->type = CLIENT_EVENT_TYPE_GET_REQUEST;
	this->socket = socket;
	this->message.get.key = key;
	this->message.get.keySize = keySize;
	this->message.get.fd = fd;
}

void ClientEvent::reqUpdate( ClientSocket *socket, char *key, uint32_t keySize, int fd, uint32_t offset ) {
	this->type = CLIENT_EVENT_TYPE_UPDATE_REQUEST;
	this->socket = socket;
	this->message.update.key = key;
	this->message.update.keySize = keySize;
	this->message.update.offset = offset;
	this->message.update.fd = fd;
}

void ClientEvent::reqDelete( ClientSocket *socket, char *key, uint32_t keySize ) {
	this->type = CLIENT_EVENT_TYPE_DELETE_REQUEST;
	this->socket = socket;
	this->message.del.key = key;
	this->message.del.keySize = keySize;
}

void ClientEvent::pending( ClientSocket *socket ) {
	this->type = CLIENT_EVENT_TYPE_PENDING;
	this->socket = socket;
}

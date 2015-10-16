#include "slave_event.hh"

void SlaveEvent::pending( SlaveSocket *socket ) {
	this->type = SLAVE_EVENT_TYPE_PENDING;
	this->socket = socket;
}

void SlaveEvent::resRegister( SlaveSocket *socket, uint32_t id, bool success ) {
	this->type = success ? SLAVE_EVENT_TYPE_REGISTER_RESPONSE_SUCCESS : SLAVE_EVENT_TYPE_REGISTER_RESPONSE_FAILURE;
	this->id = id;
	this->socket = socket;
}

void SlaveEvent::announceSlaveConnected( SlaveSocket *socket ) {
	this->type = SLAVE_EVENT_TYPE_ANNOUNCE_SLAVE_CONNECTED;
	this->socket = socket;
}

void SlaveEvent::reqSealChunks( SlaveSocket *socket ) {
	this->type = SLAVE_EVENT_TYPE_REQUEST_SEAL_CHUNKS;
	this->socket = socket;
}

void SlaveEvent::reqFlushChunks( SlaveSocket *socket ) {
	this->type = SLAVE_EVENT_TYPE_REQUEST_FLUSH_CHUNKS;
	this->socket = socket;
}

void SlaveEvent::disconnect( SlaveSocket *socket ) {
	this->type = SLAVE_EVENT_TYPE_DISCONNECT;
	this->socket = socket;
}

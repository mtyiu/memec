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

void SlaveEvent::announceSlaveReconstructed( SlaveSocket *srcSocket, SlaveSocket *dstSocket ) {
	this->type = SLAVE_EVENT_TYPE_ANNOUNCE_SLAVE_RECONSTRUCTED;
	this->message.reconstructed.src = srcSocket;
	this->message.reconstructed.dst = dstSocket;
}

void SlaveEvent::reqSealChunks( SlaveSocket *socket ) {
	this->type = SLAVE_EVENT_TYPE_REQUEST_SEAL_CHUNKS;
	this->socket = socket;
}

void SlaveEvent::reqFlushChunks( SlaveSocket *socket ) {
	this->type = SLAVE_EVENT_TYPE_REQUEST_FLUSH_CHUNKS;
	this->socket = socket;
}

void SlaveEvent::reqSyncMeta( SlaveSocket *socket, bool *sync ) {
	this->type = SLAVE_EVENT_TYPE_REQUEST_SYNC_META;
	this->socket = socket;
	this->message.sync = sync;
}

void SlaveEvent::syncRemappedParity( SlaveSocket *socket, Packet *packet ) {
	this->type = SLAVE_EVENT_TYPE_PARITY_MIGRATE;
	this->socket = socket;
	this->message.parity.packet = packet;
}

void SlaveEvent::reqReleaseDegradedLock( SlaveSocket *socket, bool *done ) {
	this->type = SLAVE_EVENT_TYPE_REQUEST_RELEASE_DEGRADED_LOCK;
	this->socket = socket;
	this->message.degraded.done = done;
}

void SlaveEvent::disconnect( SlaveSocket *socket ) {
	this->type = SLAVE_EVENT_TYPE_DISCONNECT;
	this->socket = socket;
}

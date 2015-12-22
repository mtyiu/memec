#include "coordinator_event.hh"

void CoordinatorEvent::reqRegister( CoordinatorSocket *socket, uint32_t addr, uint16_t port ) {
	this->type = COORDINATOR_EVENT_TYPE_REGISTER_REQUEST;
	this->socket = socket;
	this->message.address.addr = addr;
	this->message.address.port = port;
}

void CoordinatorEvent::sync( CoordinatorSocket *socket, uint32_t id ) {
	this->type = COORDINATOR_EVENT_TYPE_SYNC;
	this->socket = socket;
	this->id = id;
}

void CoordinatorEvent::resReleaseDegradedLock( CoordinatorSocket *socket, uint32_t id, uint32_t count ) {
	this->type = COORDINATOR_EVENT_TYPE_RELEASE_DEGRADED_LOCK_RESPONSE_SUCCESS;
	this->socket = socket;
	this->id = id;
	this->message.degraded.count = count;
}

void CoordinatorEvent::resRemappedData( CoordinatorSocket *socket, uint32_t *id ) {
	this->type = COORDINATOR_EVENT_TYPE_RESPONSE_PARITY_MIGRATE;
	if ( socket ) this->socket = socket;
	if ( id ) this->id = *id;
}

void CoordinatorEvent::resReconstruction( CoordinatorSocket *socket, uint32_t id, uint32_t listId, uint32_t chunkId, uint32_t numStripes ) {
	this->type = COORDINATOR_EVENT_TYPE_RECONSTRUCTION_RESPONSE_SUCCESS;
	this->socket = socket;
	this->id = id;
	this->message.reconstruction.listId = listId;
	this->message.reconstruction.chunkId = chunkId;
	this->message.reconstruction.numStripes = numStripes;
}

void CoordinatorEvent::resPromoteBackupSlave( CoordinatorSocket *socket, uint32_t id, uint32_t addr, uint16_t port, uint32_t count ) {
	this->type = COORDINATOR_EVENT_TYPE_PROMOTE_BACKUP_SERVER_RESPONSE_SUCCESS;
	this->socket = socket;
	this->id = id;
	this->message.promote.addr = addr;
	this->message.promote.port = port;
	this->message.promote.count = count;
}

void CoordinatorEvent::pending( CoordinatorSocket *socket ) {
	this->type = COORDINATOR_EVENT_TYPE_PENDING;
	this->socket = socket;
}

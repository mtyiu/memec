#include "coordinator_event.hh"

void CoordinatorEvent::reqRegister( CoordinatorSocket *socket, uint32_t addr, uint16_t port ) {
	this->type = COORDINATOR_EVENT_TYPE_REGISTER_REQUEST;
	this->socket = socket;
	this->message.address.addr = addr;
	this->message.address.port = port;
}

void CoordinatorEvent::sync( CoordinatorSocket *socket, uint16_t instanceId, uint32_t requestId ) {
	this->type = COORDINATOR_EVENT_TYPE_SYNC;
	this->socket = socket;
	this->instanceId = instanceId;
	this->requestId = requestId;
}

void CoordinatorEvent::resReleaseDegradedLock( CoordinatorSocket *socket, uint16_t instanceId, uint32_t requestId, uint32_t count ) {
	this->type = COORDINATOR_EVENT_TYPE_RELEASE_DEGRADED_LOCK_RESPONSE_SUCCESS;
	this->socket = socket;
	this->instanceId = instanceId;
	this->requestId = requestId;
	this->message.degraded.count = count;
}

void CoordinatorEvent::resRemappedData() {
	this->type = COORDINATOR_EVENT_TYPE_RESPONSE_PARITY_MIGRATE;
}

void CoordinatorEvent::resRemappedData( CoordinatorSocket *socket, uint16_t instanceId, uint32_t requestId ) {
	this->type = COORDINATOR_EVENT_TYPE_RESPONSE_PARITY_MIGRATE;
	this->socket = socket;
	this->instanceId = instanceId;
	this->requestId = requestId;
}

void CoordinatorEvent::resReconstruction( CoordinatorSocket *socket, uint16_t instanceId, uint32_t requestId, uint32_t listId, uint32_t chunkId, uint32_t numStripes ) {
	this->type = COORDINATOR_EVENT_TYPE_RECONSTRUCTION_RESPONSE_SUCCESS;
	this->socket = socket;
	this->instanceId = instanceId;
	this->requestId = requestId;
	this->message.reconstruction.listId = listId;
	this->message.reconstruction.chunkId = chunkId;
	this->message.reconstruction.numStripes = numStripes;
}

void CoordinatorEvent::resReconstructionUnsealed( CoordinatorSocket *socket, uint16_t instanceId, uint32_t requestId, uint32_t listId, uint32_t chunkId, uint32_t keysCount ) {
	this->type = COORDINATOR_EVENT_TYPE_RECONSTRUCTION_UNSEALED_RESPONSE_SUCCESS;
	this->socket = socket;
	this->instanceId = instanceId;
	this->requestId = requestId;
	this->message.reconstructionUnsealed.listId = listId;
	this->message.reconstructionUnsealed.chunkId = chunkId;
	this->message.reconstructionUnsealed.keysCount = keysCount;
}

void CoordinatorEvent::resPromoteBackupSlave( CoordinatorSocket *socket, uint16_t instanceId, uint32_t requestId, uint32_t addr, uint16_t port, uint32_t numChunks, uint32_t numUnsealedKeys ) {
	this->type = COORDINATOR_EVENT_TYPE_PROMOTE_BACKUP_SERVER_RESPONSE_SUCCESS;
	this->socket = socket;
	this->instanceId = instanceId;
	this->requestId = requestId;
	this->message.promote.addr = addr;
	this->message.promote.port = port;
	this->message.promote.numChunks = numChunks;
	this->message.promote.numUnsealedKeys = numUnsealedKeys;
}

void CoordinatorEvent::pending( CoordinatorSocket *socket ) {
	this->type = COORDINATOR_EVENT_TYPE_PENDING;
	this->socket = socket;
}

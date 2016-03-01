#include "server_event.hh"

void SlaveEvent::pending( ServerSocket *socket ) {
	this->type = SLAVE_EVENT_TYPE_PENDING;
	this->socket = socket;
}

void SlaveEvent::resRegister( ServerSocket *socket, uint16_t instanceId, uint32_t requestId, bool success ) {
	this->type = success ? SLAVE_EVENT_TYPE_REGISTER_RESPONSE_SUCCESS : SLAVE_EVENT_TYPE_REGISTER_RESPONSE_FAILURE;
	this->instanceId = instanceId;
	this->requestId = requestId;
	this->socket = socket;
}

void SlaveEvent::announceSlaveConnected( ServerSocket *socket ) {
	this->type = SLAVE_EVENT_TYPE_ANNOUNCE_SLAVE_CONNECTED;
	this->socket = socket;
}

void SlaveEvent::announceSlaveReconstructed(
	uint16_t instanceId, uint32_t requestId,
	pthread_mutex_t *lock, pthread_cond_t *cond, std::unordered_set<ServerSocket *> *sockets,
	ServerSocket *srcSocket, ServerSocket *dstSocket
) {
	this->type = SLAVE_EVENT_TYPE_ANNOUNCE_SLAVE_RECONSTRUCTED;
	this->instanceId = instanceId;
	this->requestId = requestId;
	this->message.reconstructed.lock = lock;
	this->message.reconstructed.cond = cond;
	this->message.reconstructed.sockets = sockets;
	this->message.reconstructed.src = srcSocket;
	this->message.reconstructed.dst = dstSocket;
}

void SlaveEvent::reqSealChunks( ServerSocket *socket ) {
	this->type = SLAVE_EVENT_TYPE_REQUEST_SEAL_CHUNKS;
	this->socket = socket;
}

void SlaveEvent::reqFlushChunks( ServerSocket *socket ) {
	this->type = SLAVE_EVENT_TYPE_REQUEST_FLUSH_CHUNKS;
	this->socket = socket;
}

void SlaveEvent::reqSyncMeta( ServerSocket *socket, bool *sync ) {
	this->type = SLAVE_EVENT_TYPE_REQUEST_SYNC_META;
	this->socket = socket;
	this->message.sync = sync;
}

void SlaveEvent::syncRemappedData( ServerSocket *socket, Packet *packet ) {
	this->type = SLAVE_EVENT_TYPE_PARITY_MIGRATE;
	this->socket = socket;
	this->message.parity.packet = packet;
}

void SlaveEvent::reqReleaseDegradedLock( ServerSocket *socket, pthread_mutex_t *lock, pthread_cond_t *cond, bool *done ) {
	this->type = SLAVE_EVENT_TYPE_REQUEST_RELEASE_DEGRADED_LOCK;
	this->socket = socket;
	this->message.degraded.lock = lock;
	this->message.degraded.cond = cond;
	this->message.degraded.done = done;
}

void SlaveEvent::resHeartbeat( ServerSocket *socket, uint32_t timestamp, uint32_t sealed, uint32_t keys, bool isLast ) {
	this->type = SLAVE_EVENT_TYPE_RESPONSE_HEARTBEAT;
	this->socket = socket;
	this->message.heartbeat.timestamp = timestamp;
	this->message.heartbeat.sealed = sealed;
	this->message.heartbeat.keys = keys;
	this->message.heartbeat.isLast = isLast;
}

void SlaveEvent::triggerReconstruction( struct sockaddr_in addr ) {
	this->type = SLAVE_EVENT_TYPE_TRIGGER_RECONSTRUCTION;
	this->message.addr = addr;
}

void SlaveEvent::disconnect( ServerSocket *socket ) {
	this->type = SLAVE_EVENT_TYPE_DISCONNECT;
	this->socket = socket;
}

void SlaveEvent::handleReconstructionRequest( ServerSocket *socket ) {
	this->type = SLAVE_EVENT_TYPE_HANDLE_RECONSTRUCTION_REQUEST;
	this->socket = socket;
}

void SlaveEvent::ackCompletedReconstruction( ServerSocket *socket, uint16_t instanceId, uint32_t requestId, bool success ) {
	this->type = success ? SLAVE_EVENT_TYPE_ACK_RECONSTRUCTION_SUCCESS : SLAVE_EVENT_TYPE_ACK_RECONSTRUCTION_FAILURE;
	this->instanceId = instanceId;
	this->requestId = requestId;
	this->socket = socket;
}

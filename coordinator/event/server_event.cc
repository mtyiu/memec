#include "server_event.hh"

void ServerEvent::pending( ServerSocket *socket ) {
	this->type = SERVER_EVENT_TYPE_PENDING;
	this->socket = socket;
}

void ServerEvent::resRegister( ServerSocket *socket, uint16_t instanceId, uint32_t requestId, bool success ) {
	this->type = success ? SERVER_EVENT_TYPE_REGISTER_RESPONSE_SUCCESS : SERVER_EVENT_TYPE_REGISTER_RESPONSE_FAILURE;
	this->instanceId = instanceId;
	this->requestId = requestId;
	this->socket = socket;
}

void ServerEvent::announceSlaveConnected( ServerSocket *socket ) {
	this->type = SERVER_EVENT_TYPE_ANNOUNCE_SERVER_CONNECTED;
	this->socket = socket;
}

void ServerEvent::announceSlaveReconstructed(
	uint16_t instanceId, uint32_t requestId,
	pthread_mutex_t *lock, pthread_cond_t *cond, std::unordered_set<ServerSocket *> *sockets,
	ServerSocket *srcSocket, ServerSocket *dstSocket
) {
	this->type = SERVER_EVENT_TYPE_ANNOUNCE_SERVER_RECONSTRUCTED;
	this->instanceId = instanceId;
	this->requestId = requestId;
	this->message.reconstructed.lock = lock;
	this->message.reconstructed.cond = cond;
	this->message.reconstructed.sockets = sockets;
	this->message.reconstructed.src = srcSocket;
	this->message.reconstructed.dst = dstSocket;
}

void ServerEvent::reqSealChunks( ServerSocket *socket ) {
	this->type = SERVER_EVENT_TYPE_REQUEST_SEAL_CHUNKS;
	this->socket = socket;
}

void ServerEvent::reqFlushChunks( ServerSocket *socket ) {
	this->type = SERVER_EVENT_TYPE_REQUEST_FLUSH_CHUNKS;
	this->socket = socket;
}

void ServerEvent::reqSyncMeta( ServerSocket *socket, bool *sync ) {
	this->type = SERVER_EVENT_TYPE_REQUEST_SYNC_META;
	this->socket = socket;
	this->message.sync = sync;
}

void ServerEvent::syncRemappedData( ServerSocket *socket, Packet *packet ) {
	this->type = SERVER_EVENT_TYPE_PARITY_MIGRATE;
	this->socket = socket;
	this->message.parity.packet = packet;
}

void ServerEvent::reqReleaseDegradedLock( ServerSocket *socket, pthread_mutex_t *lock, pthread_cond_t *cond, bool *done ) {
	this->type = SERVER_EVENT_TYPE_REQUEST_RELEASE_DEGRADED_LOCK;
	this->socket = socket;
	this->message.degraded.lock = lock;
	this->message.degraded.cond = cond;
	this->message.degraded.done = done;
}

void ServerEvent::resHeartbeat( ServerSocket *socket, uint32_t timestamp, uint32_t sealed, uint32_t keys, bool isLast ) {
	this->type = SERVER_EVENT_TYPE_RESPONSE_HEARTBEAT;
	this->socket = socket;
	this->message.heartbeat.timestamp = timestamp;
	this->message.heartbeat.sealed = sealed;
	this->message.heartbeat.keys = keys;
	this->message.heartbeat.isLast = isLast;
}

void ServerEvent::triggerReconstruction( struct sockaddr_in addr ) {
	this->type = SERVER_EVENT_TYPE_TRIGGER_RECONSTRUCTION;
	this->message.addr = addr;
}

void ServerEvent::disconnect( ServerSocket *socket ) {
	this->type = SERVER_EVENT_TYPE_DISCONNECT;
	this->socket = socket;
}

void ServerEvent::handleReconstructionRequest( ServerSocket *socket ) {
	this->type = SERVER_EVENT_TYPE_HANDLE_RECONSTRUCTION_REQUEST;
	this->socket = socket;
}

void ServerEvent::ackCompletedReconstruction( ServerSocket *socket, uint16_t instanceId, uint32_t requestId, bool success ) {
	this->type = success ? SERVER_EVENT_TYPE_ACK_RECONSTRUCTION_SUCCESS : SERVER_EVENT_TYPE_ACK_RECONSTRUCTION_FAILURE;
	this->instanceId = instanceId;
	this->requestId = requestId;
	this->socket = socket;
}

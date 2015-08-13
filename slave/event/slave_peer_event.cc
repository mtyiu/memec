#include "slave_peer_event.hh"

void SlavePeerEvent::reqRegister( SlavePeerSocket *socket ) {
	this->type = SLAVE_PEER_EVENT_TYPE_REGISTER_REQUEST;
	this->socket = socket;
}

void SlavePeerEvent::resRegister( SlavePeerSocket *socket, bool success ) {
	this->type = success ? SLAVE_PEER_EVENT_TYPE_REGISTER_RESPONSE_SUCCESS : SLAVE_PEER_EVENT_TYPE_REGISTER_RESPONSE_FAILURE;
	this->socket = socket;
}

void SlavePeerEvent::reqUpdateChunk( SlavePeerSocket *socket, Metadata &metadata, uint32_t offset, uint32_t length, uint32_t updatingChunkId, volatile bool *status, char *delta ) {
	this->type = SLAVE_PEER_EVENT_TYPE_UPDATE_CHUNK_REQUEST;
	this->socket = socket;
	this->message.chunkUpdate.metadata = metadata;
	this->message.chunkUpdate.offset = offset;
	this->message.chunkUpdate.length = length;
	this->message.chunkUpdate.updatingChunkId = updatingChunkId;
	this->message.chunkUpdate.status = status;
	this->message.chunkUpdate.delta = delta;
}

void SlavePeerEvent::resUpdateChunk( SlavePeerSocket *socket, Metadata &metadata, uint32_t offset, uint32_t length, uint32_t updatingChunkId, bool success ) {
	this->type = success ? SLAVE_PEER_EVENT_TYPE_UPDATE_CHUNK_RESPONSE_SUCCESS : SLAVE_PEER_EVENT_TYPE_UPDATE_CHUNK_RESPONSE_FAILURE;
	this->socket = socket;
	this->message.chunkUpdate.metadata = metadata;
	this->message.chunkUpdate.offset = offset;
	this->message.chunkUpdate.length = length;
	this->message.chunkUpdate.updatingChunkId = updatingChunkId;
}

void SlavePeerEvent::reqDeleteChunk( SlavePeerSocket *socket, Metadata &metadata, uint32_t offset, uint32_t length, uint32_t updatingChunkId, volatile bool *status, char *delta ) {
	this->type = SLAVE_PEER_EVENT_TYPE_DELETE_CHUNK_REQUEST;
	this->socket = socket;
	this->message.chunkUpdate.metadata = metadata;
	this->message.chunkUpdate.offset = offset;
	this->message.chunkUpdate.length = length;
	this->message.chunkUpdate.updatingChunkId = updatingChunkId;
	this->message.chunkUpdate.status = status;
	this->message.chunkUpdate.delta = delta;
}

void SlavePeerEvent::resDeleteChunk( SlavePeerSocket *socket, Metadata &metadata, uint32_t offset, uint32_t length, uint32_t updatingChunkId, bool success ) {
	this->type = success ? SLAVE_PEER_EVENT_TYPE_DELETE_CHUNK_RESPONSE_SUCCESS : SLAVE_PEER_EVENT_TYPE_DELETE_CHUNK_RESPONSE_FAILURE;
	this->socket = socket;
	this->message.chunkUpdate.metadata = metadata;
	this->message.chunkUpdate.offset = offset;
	this->message.chunkUpdate.length = length;
	this->message.chunkUpdate.updatingChunkId = updatingChunkId;
}

void SlavePeerEvent::pending( SlavePeerSocket *socket ) {
	this->type = SLAVE_PEER_EVENT_TYPE_PENDING;
	this->socket = socket;
}

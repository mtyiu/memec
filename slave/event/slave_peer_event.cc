#include "slave_peer_event.hh"

void SlavePeerEvent::reqRegister( SlavePeerSocket *socket ) {
	this->type = SLAVE_PEER_EVENT_TYPE_REGISTER_REQUEST;
	this->socket = socket;
}

void SlavePeerEvent::resRegister( SlavePeerSocket *socket, bool success ) {
	this->type = success ? SLAVE_PEER_EVENT_TYPE_REGISTER_RESPONSE_SUCCESS : SLAVE_PEER_EVENT_TYPE_REGISTER_RESPONSE_FAILURE;
	this->socket = socket;
}

void SlavePeerEvent::resUpdateChunk( SlavePeerSocket *socket, Metadata &metadata, uint32_t offset, uint32_t length, uint32_t updatingChunkId, bool success ) {
	this->type = success ? SLAVE_PEER_EVENT_TYPE_UPDATE_CHUNK_RESPONSE_SUCCESS : SLAVE_PEER_EVENT_TYPE_UPDATE_CHUNK_RESPONSE_FAILURE;
	this->socket = socket;
	this->message.chunkUpdate.metadata = metadata;
	this->message.chunkUpdate.offset = offset;
	this->message.chunkUpdate.length = length;
	this->message.chunkUpdate.updatingChunkId = updatingChunkId;
}

void SlavePeerEvent::resDeleteChunk( SlavePeerSocket *socket, Metadata &metadata, uint32_t offset, uint32_t length, uint32_t updatingChunkId, bool success ) {
	this->type = success ? SLAVE_PEER_EVENT_TYPE_DELETE_CHUNK_RESPONSE_SUCCESS : SLAVE_PEER_EVENT_TYPE_DELETE_CHUNK_RESPONSE_FAILURE;
	this->socket = socket;
	this->message.chunkUpdate.metadata = metadata;
	this->message.chunkUpdate.offset = offset;
	this->message.chunkUpdate.length = length;
	this->message.chunkUpdate.updatingChunkId = updatingChunkId;
}

void SlavePeerEvent::reqGetChunk( SlavePeerSocket *socket, Metadata &metadata, volatile bool *status ) {
	this->type = SLAVE_PEER_EVENT_TYPE_GET_CHUNK_REQUEST;
	this->socket = socket;
	this->message.chunk.metadata = metadata;
	this->message.chunk.status = status;
	this->message.chunk.chunk = 0;
}

void SlavePeerEvent::resGetChunk( SlavePeerSocket *socket, Metadata &metadata, bool success, Chunk *chunk ) {
	this->type = success ? SLAVE_PEER_EVENT_TYPE_GET_CHUNK_RESPONSE_SUCCESS : SLAVE_PEER_EVENT_TYPE_GET_CHUNK_RESPONSE_FAILURE;
	this->socket = socket;
	this->message.chunk.metadata = metadata;
	this->message.chunk.status = 0;
	this->message.chunk.chunk = chunk;
}

void SlavePeerEvent::reqSetChunk( SlavePeerSocket *socket, Metadata &metadata, Chunk *chunk, volatile bool *status ) {
	this->type = SLAVE_PEER_EVENT_TYPE_SET_CHUNK_REQUEST;
	this->socket = socket;
	this->message.chunk.metadata = metadata;
	this->message.chunk.chunk = chunk;
	this->message.chunk.status = status;
}

void SlavePeerEvent::resSetChunk( SlavePeerSocket *socket, Metadata &metadata, bool success ) {
	this->type = success ? SLAVE_PEER_EVENT_TYPE_SET_CHUNK_RESPONSE_SUCCESS : SLAVE_PEER_EVENT_TYPE_SET_CHUNK_RESPONSE_FAILURE;
	this->socket = socket;
	this->message.chunk.metadata = metadata;
	this->message.chunk.status = 0;
	this->message.chunk.chunk = 0;
}

void SlavePeerEvent::send( SlavePeerSocket *socket, Packet *packet ) {
	this->type = SLAVE_PEER_EVENT_TYPE_SEND;
	this->socket = socket;
	this->message.send.packet = packet;
}

void SlavePeerEvent::pending( SlavePeerSocket *socket ) {
	this->type = SLAVE_PEER_EVENT_TYPE_PENDING;
	this->socket = socket;
}

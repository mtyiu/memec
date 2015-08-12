#include "slave_peer_event.hh"

void SlavePeerEvent::reqRegister( SlavePeerSocket *socket ) {
	this->type = SLAVE_PEER_EVENT_TYPE_REGISTER_REQUEST;
	this->socket = socket;
}

void SlavePeerEvent::resRegister( SlavePeerSocket *socket, bool success ) {
	this->type = success ? SLAVE_PEER_EVENT_TYPE_REGISTER_RESPONSE_SUCCESS : SLAVE_PEER_EVENT_TYPE_REGISTER_RESPONSE_FAILURE;
	this->socket = socket;
}

void SlavePeerEvent::resUpdateChunk( SlavePeerSocket *socket, Metadata &metadata, uint32_t offset, uint32_t length, bool success ) {
	this->type = success ? SLAVE_PEER_EVENT_TYPE_UPDATE_CHUNK_RESPONSE_SUCCESS : SLAVE_PEER_EVENT_TYPE_UPDATE_CHUNK_RESPONSE_FAILURE;
	this->socket = socket;
	this->message.chunkUpdate.metadata = metadata;
	this->message.chunkUpdate.offset = offset;
	this->message.chunkUpdate.length = length;
}

void SlavePeerEvent::resDeleteChunk( SlavePeerSocket *socket, Metadata &metadata, uint32_t offset, uint32_t length, bool success ) {
	this->type = success ? SLAVE_PEER_EVENT_TYPE_DELETE_CHUNK_RESPONSE_SUCCESS : SLAVE_PEER_EVENT_TYPE_DELETE_CHUNK_RESPONSE_FAILURE;
	this->socket = socket;
	this->message.chunkUpdate.metadata = metadata;
	this->message.chunkUpdate.offset = offset;
	this->message.chunkUpdate.length = length;
}

void SlavePeerEvent::send( SlavePeerSocket *socket, SlaveProtocol *protocol, size_t size, size_t index ) {
	this->type = SLAVE_PEER_EVENT_TYPE_SEND;
	this->socket = socket;
	this->message.send.protocol = protocol;
	this->message.send.size = size;
	this->message.send.index = index;
}

void SlavePeerEvent::pending( SlavePeerSocket *socket ) {
	this->type = SLAVE_PEER_EVENT_TYPE_PENDING;
	this->socket = socket;
}

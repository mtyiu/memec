#include "slave_peer_event.hh"
#include "../buffer/mixed_chunk_buffer.hh"

void SlavePeerEvent::reqRegister( SlavePeerSocket *socket ) {
	this->type = SLAVE_PEER_EVENT_TYPE_REGISTER_REQUEST;
	this->socket = socket;
}

void SlavePeerEvent::resRegister( SlavePeerSocket *socket, uint16_t instanceId, uint32_t requestId, bool success ) {
	this->type = success ? SLAVE_PEER_EVENT_TYPE_REGISTER_RESPONSE_SUCCESS : SLAVE_PEER_EVENT_TYPE_REGISTER_RESPONSE_FAILURE;
	this->instanceId = instanceId;
	this->requestId = requestId;
	this->socket = socket;
}

void SlavePeerEvent::resRemappingSet( SlavePeerSocket *socket, uint16_t instanceId, uint32_t requestId, Key &key, uint32_t listId, uint32_t chunkId, bool success ) {
	this->type = success ? SLAVE_PEER_EVENT_TYPE_REMAPPING_SET_RESPONSE_SUCCESS : SLAVE_PEER_EVENT_TYPE_REMAPPING_SET_RESPONSE_FAILURE;
	this->instanceId = instanceId;
	this->requestId = requestId;
	this->socket = socket;
	this->message.remap.key = key;
	this->message.remap.listId = listId;
	this->message.remap.chunkId = chunkId;
}

void SlavePeerEvent::reqSet( SlavePeerSocket *socket, uint16_t instanceId, uint32_t requestId, Key key, Value value ) {
	this->type = SLAVE_PEER_EVENT_TYPE_SET_REQUEST;
	this->instanceId = instanceId;
	this->requestId = requestId;
	this->socket = socket;
	this->message.set.key = key;
	this->message.set.value = value;
}

void SlavePeerEvent::resSet( SlavePeerSocket *socket, uint16_t instanceId, uint32_t requestId, Key key, bool success ) {
	this->type = success ? SLAVE_PEER_EVENT_TYPE_SET_RESPONSE_SUCCESS : SLAVE_PEER_EVENT_TYPE_SET_RESPONSE_FAILURE;
	this->instanceId = instanceId;
	this->requestId = requestId;
	this->socket = socket;
	this->message.set.key = key;
}

void SlavePeerEvent::reqDegradedSet(
	SlavePeerSocket *socket,
	uint8_t opcode,
	uint16_t instanceId, uint32_t requestId,
	uint32_t listId, uint32_t chunkId,
	uint8_t keySize, uint32_t valueSize,
	char *key, char *value,
	uint32_t valueUpdateOffset, uint32_t valueUpdateSize, char *valueUpdate
) {
	this->type = SLAVE_PEER_EVENT_TYPE_DEGRADED_SET_REQUEST;
	this->instanceId = instanceId;
	this->requestId = requestId;
	this->socket = socket;
	this->message.degradedSet.opcode = opcode;
	this->message.degradedSet.listId = listId;
	this->message.degradedSet.chunkId = chunkId;
	this->message.degradedSet.keySize = keySize;
	this->message.degradedSet.valueSize = valueSize;
	this->message.degradedSet.key = key;
	this->message.degradedSet.value = value;
	if ( opcode == PROTO_OPCODE_DEGRADED_UPDATE ) {
		this->message.degradedSet.update.offset = valueUpdateOffset;
		this->message.degradedSet.update.length = valueUpdateSize;
		this->message.degradedSet.update.data = valueUpdate;
	} else {
		this->message.degradedSet.update.offset = 0;
		this->message.degradedSet.update.length = 0;
		this->message.degradedSet.update.data = 0;
	}
}

void SlavePeerEvent::reqGet( SlavePeerSocket *socket, uint16_t instanceId, uint32_t requestId, uint32_t listId, uint32_t chunkId, Key &key ) {
	this->type = SLAVE_PEER_EVENT_TYPE_GET_REQUEST;
	this->instanceId = instanceId;
	this->requestId = requestId;
	this->socket = socket;
	this->message.get.listId = listId;
	this->message.get.chunkId = chunkId;
	this->message.get.key = key;
}

void SlavePeerEvent::resGet( SlavePeerSocket *socket, uint16_t instanceId, uint32_t requestId, KeyValue &keyValue ) {
	this->type = SLAVE_PEER_EVENT_TYPE_GET_RESPONSE_SUCCESS;
	this->instanceId = instanceId;
	this->requestId = requestId;
	this->socket = socket;
	this->message.get.keyValue = keyValue;
}

void SlavePeerEvent::resGet( SlavePeerSocket *socket, uint16_t instanceId, uint32_t requestId, Key &key ) {
	this->type = SLAVE_PEER_EVENT_TYPE_GET_RESPONSE_FAILURE;
	this->instanceId = instanceId;
	this->requestId = requestId;
	this->socket = socket;
	this->message.get.key = key;
}

void SlavePeerEvent::resUpdate( SlavePeerSocket *socket, uint16_t instanceId, uint32_t requestId, uint32_t listId, uint32_t stripeId, uint32_t chunkId, Key &key, uint32_t valueUpdateOffset, uint32_t length, uint32_t chunkUpdateOffset, bool success ) {
	this->type = success ? SLAVE_PEER_EVENT_TYPE_UPDATE_RESPONSE_SUCCESS : SLAVE_PEER_EVENT_TYPE_UPDATE_RESPONSE_FAILURE;
	this->instanceId = instanceId;
	this->requestId = requestId;
	this->socket = socket;
	this->message.update.listId = listId;
	this->message.update.stripeId = stripeId;
	this->message.update.chunkId = chunkId;
	this->message.update.valueUpdateOffset = valueUpdateOffset;
	this->message.update.chunkUpdateOffset = chunkUpdateOffset;
	this->message.update.length = length;
	this->message.update.key = key;
}

void SlavePeerEvent::resDelete( SlavePeerSocket *socket, uint16_t instanceId, uint32_t requestId, uint32_t listId, uint32_t stripeId, uint32_t chunkId, Key &key, bool success ) {
	this->type = success ? SLAVE_PEER_EVENT_TYPE_DELETE_RESPONSE_SUCCESS : SLAVE_PEER_EVENT_TYPE_DELETE_RESPONSE_FAILURE;
	this->instanceId = instanceId;
	this->requestId = requestId;
	this->socket = socket;
	this->message.del.listId = listId;
	this->message.del.stripeId = stripeId;
	this->message.del.chunkId = chunkId;
	this->message.del.key = key;
}

void SlavePeerEvent::resUpdateChunk( SlavePeerSocket *socket, uint16_t instanceId, uint32_t requestId, Metadata &metadata, uint32_t offset, uint32_t length, uint32_t updatingChunkId, bool success ) {
	this->type = success ? SLAVE_PEER_EVENT_TYPE_UPDATE_CHUNK_RESPONSE_SUCCESS : SLAVE_PEER_EVENT_TYPE_UPDATE_CHUNK_RESPONSE_FAILURE;
	this->instanceId = instanceId;
	this->requestId = requestId;
	this->socket = socket;
	this->message.chunkUpdate.metadata = metadata;
	this->message.chunkUpdate.offset = offset;
	this->message.chunkUpdate.length = length;
	this->message.chunkUpdate.updatingChunkId = updatingChunkId;
}

void SlavePeerEvent::resDeleteChunk( SlavePeerSocket *socket, uint16_t instanceId, uint32_t requestId, Metadata &metadata, uint32_t offset, uint32_t length, uint32_t updatingChunkId, bool success ) {
	this->type = success ? SLAVE_PEER_EVENT_TYPE_DELETE_CHUNK_RESPONSE_SUCCESS : SLAVE_PEER_EVENT_TYPE_DELETE_CHUNK_RESPONSE_FAILURE;
	this->instanceId = instanceId;
	this->requestId = requestId;
	this->socket = socket;
	this->message.chunkUpdate.metadata = metadata;
	this->message.chunkUpdate.offset = offset;
	this->message.chunkUpdate.length = length;
	this->message.chunkUpdate.updatingChunkId = updatingChunkId;
}

void SlavePeerEvent::reqGetChunk( SlavePeerSocket *socket, uint16_t instanceId, uint32_t requestId, Metadata &metadata ) {
	this->type = SLAVE_PEER_EVENT_TYPE_GET_CHUNK_REQUEST;
	this->instanceId = instanceId;
	this->requestId = requestId;
	this->socket = socket;
	this->message.chunk.metadata = metadata;
	this->message.chunk.chunk = 0;
}

void SlavePeerEvent::resGetChunk( SlavePeerSocket *socket, uint16_t instanceId, uint32_t requestId, Metadata &metadata, bool success, Chunk *chunk ) {
	this->type = success ? SLAVE_PEER_EVENT_TYPE_GET_CHUNK_RESPONSE_SUCCESS : SLAVE_PEER_EVENT_TYPE_GET_CHUNK_RESPONSE_FAILURE;
	this->instanceId = instanceId;
	this->requestId = requestId;
	this->socket = socket;
	this->message.chunk.metadata = metadata;
	this->message.chunk.chunk = chunk;
}

void SlavePeerEvent::batchGetChunks( SlavePeerSocket *socket, std::vector<uint32_t> *requestIds, std::vector<Metadata> *metadata ) {
	this->type = SLAVE_PEER_EVENT_TYPE_BATCH_GET_CHUNKS;
	this->socket = socket;
	this->message.batchGetChunks.requestIds = requestIds;
	this->message.batchGetChunks.metadata = metadata;
}

void SlavePeerEvent::reqSetChunk( SlavePeerSocket *socket, uint16_t instanceId, uint32_t requestId, Metadata &metadata, Chunk *chunk, bool needsFree ) {
	this->type = SLAVE_PEER_EVENT_TYPE_SET_CHUNK_REQUEST;
	this->instanceId = instanceId;
	this->requestId = requestId;
	this->socket = socket;
	this->message.chunk.metadata = metadata;
	this->message.chunk.chunk = chunk;
	this->message.chunk.needsFree = needsFree;
}

void SlavePeerEvent::resSetChunk( SlavePeerSocket *socket, uint16_t instanceId, uint32_t requestId, Metadata &metadata, bool success ) {
	this->type = success ? SLAVE_PEER_EVENT_TYPE_SET_CHUNK_RESPONSE_SUCCESS : SLAVE_PEER_EVENT_TYPE_SET_CHUNK_RESPONSE_FAILURE;
	this->instanceId = instanceId;
	this->requestId = requestId;
	this->socket = socket;
	this->message.chunk.metadata = metadata;
	this->message.chunk.chunk = 0;
}

void SlavePeerEvent::reqSealChunk( Chunk *chunk ) {
	this->type = SLAVE_PEER_EVENT_TYPE_SEAL_CHUNK_REQUEST;
	this->message.chunk.chunk = chunk;
}

void SlavePeerEvent::reqSealChunks( MixedChunkBuffer *chunkBuffer ) {
	this->type = SLAVE_PEER_EVENT_TYPE_SEAL_CHUNKS;
	this->message.chunkBuffer = chunkBuffer;
}

void SlavePeerEvent::resSealChunk( SlavePeerSocket *socket, uint16_t instanceId, uint32_t requestId, Metadata &metadata, bool success ) {
	this->type = success ? SLAVE_PEER_EVENT_TYPE_SEAL_CHUNK_RESPONSE_SUCCESS : SLAVE_PEER_EVENT_TYPE_SEAL_CHUNK_RESPONSE_FAILURE;
	this->instanceId = instanceId;
	this->requestId = requestId;
	this->socket = socket;
	this->message.chunk.metadata = metadata;
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

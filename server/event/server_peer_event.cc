#include "server_peer_event.hh"
#include "../buffer/mixed_chunk_buffer.hh"

void ServerPeerEvent::reqRegister( ServerPeerSocket *socket ) {
	this->type = SERVER_PEER_EVENT_TYPE_REGISTER_REQUEST;
	this->socket = socket;
}

void ServerPeerEvent::resRegister( ServerPeerSocket *socket, uint16_t instanceId, uint32_t requestId, bool success ) {
	this->type = success ? SERVER_PEER_EVENT_TYPE_REGISTER_RESPONSE_SUCCESS : SERVER_PEER_EVENT_TYPE_REGISTER_RESPONSE_FAILURE;
	this->set( instanceId, requestId, socket );
}

void ServerPeerEvent::resDegradedSet( ServerPeerSocket *socket, uint16_t instanceId, uint32_t requestId, Key &key, uint32_t listId, uint32_t chunkId, bool success ) {
	this->type = success ? SERVER_PEER_EVENT_TYPE_DEGRADED_SET_RESPONSE_SUCCESS : SERVER_PEER_EVENT_TYPE_DEGRADED_SET_RESPONSE_FAILURE;
	this->set( instanceId, requestId, socket );
	this->message.remap.key = key;
	this->message.remap.listId = listId;
	this->message.remap.chunkId = chunkId;
}

void ServerPeerEvent::reqSet( ServerPeerSocket *socket, uint16_t instanceId, uint32_t requestId, Key key, Value value ) {
	this->type = SERVER_PEER_EVENT_TYPE_SET_REQUEST;
	this->set( instanceId, requestId, socket );
	this->message.set.key = key;
	this->message.set.value = value;
}

void ServerPeerEvent::resSet( ServerPeerSocket *socket, uint16_t instanceId, uint32_t requestId, Key key, bool success ) {
	this->type = success ? SERVER_PEER_EVENT_TYPE_SET_RESPONSE_SUCCESS : SERVER_PEER_EVENT_TYPE_SET_RESPONSE_FAILURE;
	this->set( instanceId, requestId, socket );
	this->message.set.key = key;
}

void ServerPeerEvent::reqForwardKey(
	ServerPeerSocket *socket,
	uint8_t opcode,
	uint16_t instanceId, uint32_t requestId,
	uint32_t listId, uint32_t stripeId, uint32_t chunkId,
	uint8_t keySize, uint32_t valueSize,
	char *key, char *value,
	uint32_t valueUpdateOffset, uint32_t valueUpdateSize, char *valueUpdate
) {
	this->type = SERVER_PEER_EVENT_TYPE_FORWARD_KEY_REQUEST;
	this->set( instanceId, requestId, socket );
	this->message.forwardKey.opcode = opcode;
	this->message.forwardKey.listId = listId;
	this->message.forwardKey.stripeId = stripeId;
	this->message.forwardKey.chunkId = chunkId;
	this->message.forwardKey.keySize = keySize;
	this->message.forwardKey.valueSize = valueSize;
	this->message.forwardKey.key = key;
	this->message.forwardKey.value = value;
	if ( opcode == PROTO_OPCODE_DEGRADED_UPDATE ) {
		this->message.forwardKey.update.offset = valueUpdateOffset;
		this->message.forwardKey.update.length = valueUpdateSize;
		this->message.forwardKey.update.data = valueUpdate;
	} else {
		this->message.forwardKey.update.offset = 0;
		this->message.forwardKey.update.length = 0;
		this->message.forwardKey.update.data = 0;
	}
}

void ServerPeerEvent::resForwardKey(
	ServerPeerSocket *socket, bool success,
	uint8_t opcode,
	uint16_t instanceId, uint32_t requestId,
	uint32_t listId, uint32_t stripeId, uint32_t chunkId,
	uint8_t keySize, uint32_t valueSize,
	char *key,
	uint32_t valueUpdateOffset, uint32_t valueUpdateSize
) {
	this->type = success ? SERVER_PEER_EVENT_TYPE_FORWARD_KEY_RESPONSE_SUCCESS : SERVER_PEER_EVENT_TYPE_FORWARD_KEY_RESPONSE_FAILURE;
	this->set( instanceId, requestId, socket );
	this->message.forwardKey.opcode = opcode;
	this->message.forwardKey.listId = listId;
	this->message.forwardKey.stripeId = stripeId;
	this->message.forwardKey.chunkId = chunkId;
	this->message.forwardKey.keySize = keySize;
	this->message.forwardKey.valueSize = valueSize;
	this->message.forwardKey.key = key;
	if ( opcode == PROTO_OPCODE_DEGRADED_UPDATE ) {
		this->message.forwardKey.update.offset = valueUpdateOffset;
		this->message.forwardKey.update.length = valueUpdateSize;
	} else {
		this->message.forwardKey.update.offset = 0;
		this->message.forwardKey.update.length = 0;
	}
	this->message.forwardKey.value = 0;
	this->message.forwardKey.update.data = 0;
}

void ServerPeerEvent::reqGet( ServerPeerSocket *socket, uint16_t instanceId, uint32_t requestId, uint32_t listId, uint32_t chunkId, Key &key ) {
	this->type = SERVER_PEER_EVENT_TYPE_GET_REQUEST;
	this->set( instanceId, requestId, socket );
	this->message.get.listId = listId;
	this->message.get.chunkId = chunkId;
	this->message.get.key = key;
}

void ServerPeerEvent::resGet( ServerPeerSocket *socket, uint16_t instanceId, uint32_t requestId, KeyValue &keyValue ) {
	this->type = SERVER_PEER_EVENT_TYPE_GET_RESPONSE_SUCCESS;
	this->set( instanceId, requestId, socket );
	this->message.get.keyValue = keyValue;
}

void ServerPeerEvent::resGet( ServerPeerSocket *socket, uint16_t instanceId, uint32_t requestId, Key &key ) {
	this->type = SERVER_PEER_EVENT_TYPE_GET_RESPONSE_FAILURE;
	this->set( instanceId, requestId, socket );
	this->message.get.key = key;
}

void ServerPeerEvent::resUpdate( ServerPeerSocket *socket, uint16_t instanceId, uint32_t requestId, uint32_t listId, uint32_t stripeId, uint32_t chunkId, Key &key, uint32_t valueUpdateOffset, uint32_t length, uint32_t chunkUpdateOffset, bool success ) {
	this->type = success ? SERVER_PEER_EVENT_TYPE_UPDATE_RESPONSE_SUCCESS : SERVER_PEER_EVENT_TYPE_UPDATE_RESPONSE_FAILURE;
	this->set( instanceId, requestId, socket );
	this->message.update.listId = listId;
	this->message.update.stripeId = stripeId;
	this->message.update.chunkId = chunkId;
	this->message.update.valueUpdateOffset = valueUpdateOffset;
	this->message.update.chunkUpdateOffset = chunkUpdateOffset;
	this->message.update.length = length;
	this->message.update.key = key;
}

void ServerPeerEvent::resDelete( ServerPeerSocket *socket, uint16_t instanceId, uint32_t requestId, uint32_t listId, uint32_t stripeId, uint32_t chunkId, Key &key, bool success ) {
	this->type = success ? SERVER_PEER_EVENT_TYPE_DELETE_RESPONSE_SUCCESS : SERVER_PEER_EVENT_TYPE_DELETE_RESPONSE_FAILURE;
	this->set( instanceId, requestId, socket );
	this->message.del.listId = listId;
	this->message.del.stripeId = stripeId;
	this->message.del.chunkId = chunkId;
	this->message.del.key = key;
}

void ServerPeerEvent::resRemappedUpdate( ServerPeerSocket *socket, uint16_t instanceId, uint32_t requestId, Key &key, uint32_t valueUpdateOffset, uint32_t valueUpdateSize, bool success ) {
	this->type = success ? SERVER_PEER_EVENT_TYPE_REMAPPED_UPDATE_RESPONSE_SUCCESS : SERVER_PEER_EVENT_TYPE_REMAPPED_UPDATE_RESPONSE_FAILURE;
	this->set( instanceId, requestId, socket );
	this->message.remappingUpdate.key = key;
	this->message.remappingUpdate.valueUpdateOffset = valueUpdateOffset;
	this->message.remappingUpdate.valueUpdateSize = valueUpdateSize;
}

void ServerPeerEvent::resRemappedDelete( ServerPeerSocket *socket, uint16_t instanceId, uint32_t requestId, Key &key, bool success ) {
	this->type = success ? SERVER_PEER_EVENT_TYPE_REMAPPED_DELETE_RESPONSE_SUCCESS : SERVER_PEER_EVENT_TYPE_REMAPPED_DELETE_RESPONSE_FAILURE;
	this->set( instanceId, requestId, socket );
	this->message.remappingDel.key = key;
}

void ServerPeerEvent::resUpdateChunk( ServerPeerSocket *socket, uint16_t instanceId, uint32_t requestId, Metadata &metadata, uint32_t offset, uint32_t length, uint32_t updatingChunkId, bool success ) {
	this->type = success ? SERVER_PEER_EVENT_TYPE_UPDATE_CHUNK_RESPONSE_SUCCESS : SERVER_PEER_EVENT_TYPE_UPDATE_CHUNK_RESPONSE_FAILURE;
	this->set( instanceId, requestId, socket );
	this->message.chunkUpdate.metadata = metadata;
	this->message.chunkUpdate.offset = offset;
	this->message.chunkUpdate.length = length;
	this->message.chunkUpdate.updatingChunkId = updatingChunkId;
}

void ServerPeerEvent::resDeleteChunk( ServerPeerSocket *socket, uint16_t instanceId, uint32_t requestId, Metadata &metadata, uint32_t offset, uint32_t length, uint32_t updatingChunkId, bool success ) {
	this->type = success ? SERVER_PEER_EVENT_TYPE_DELETE_CHUNK_RESPONSE_SUCCESS : SERVER_PEER_EVENT_TYPE_DELETE_CHUNK_RESPONSE_FAILURE;
	this->set( instanceId, requestId, socket );
	this->message.chunkUpdate.metadata = metadata;
	this->message.chunkUpdate.offset = offset;
	this->message.chunkUpdate.length = length;
	this->message.chunkUpdate.updatingChunkId = updatingChunkId;
}

void ServerPeerEvent::reqGetChunk( ServerPeerSocket *socket, uint16_t instanceId, uint32_t requestId, Metadata &metadata ) {
	this->type = SERVER_PEER_EVENT_TYPE_GET_CHUNK_REQUEST;
	this->set( instanceId, requestId, socket );
	this->message.chunk.metadata = metadata;
	this->message.chunk.chunk = 0;
}

void ServerPeerEvent::resGetChunk( ServerPeerSocket *socket, uint16_t instanceId, uint32_t requestId, Metadata &metadata, bool success, uint32_t chunkBufferIndex, Chunk *chunk, uint8_t sealIndicatorCount, bool *sealIndicator ) {
	this->type = success ? SERVER_PEER_EVENT_TYPE_GET_CHUNK_RESPONSE_SUCCESS : SERVER_PEER_EVENT_TYPE_GET_CHUNK_RESPONSE_FAILURE;
	this->set( instanceId, requestId, socket );
	this->message.chunk.metadata = metadata;
	this->message.chunk.chunk = chunk;
	this->message.chunk.chunkBufferIndex = chunkBufferIndex;
	this->message.chunk.sealIndicatorCount = sealIndicatorCount;
	this->message.chunk.sealIndicator = sealIndicator;
}

void ServerPeerEvent::batchGetChunks( ServerPeerSocket *socket, std::vector<uint32_t> *requestIds, std::vector<Metadata> *metadata ) {
	this->type = SERVER_PEER_EVENT_TYPE_BATCH_GET_CHUNKS;
	this->socket = socket;
	this->message.batchGetChunks.requestIds = requestIds;
	this->message.batchGetChunks.metadata = metadata;
}

void ServerPeerEvent::reqSetChunk( ServerPeerSocket *socket, uint16_t instanceId, uint32_t requestId, Metadata &metadata, Chunk *chunk, bool needsFree ) {
	this->type = SERVER_PEER_EVENT_TYPE_SET_CHUNK_REQUEST;
	this->set( instanceId, requestId, socket );
	this->message.chunk.metadata = metadata;
	this->message.chunk.chunk = chunk;
	this->message.chunk.needsFree = needsFree;
}

void ServerPeerEvent::resSetChunk( ServerPeerSocket *socket, uint16_t instanceId, uint32_t requestId, Metadata &metadata, bool success ) {
	this->type = success ? SERVER_PEER_EVENT_TYPE_SET_CHUNK_RESPONSE_SUCCESS : SERVER_PEER_EVENT_TYPE_SET_CHUNK_RESPONSE_FAILURE;
	this->set( instanceId, requestId, socket );
	this->message.chunk.metadata = metadata;
	this->message.chunk.chunk = 0;
}

void ServerPeerEvent::reqForwardChunk( ServerPeerSocket *socket, uint16_t instanceId, uint32_t requestId, Metadata &metadata, Chunk *chunk, bool needsFree ) {
	this->type = SERVER_PEER_EVENT_TYPE_FORWARD_CHUNK_REQUEST;
	this->set( instanceId, requestId, socket );
	this->message.chunk.metadata = metadata;
	this->message.chunk.chunk = chunk;
	this->message.chunk.needsFree = needsFree;
}

void ServerPeerEvent::resForwardChunk( ServerPeerSocket *socket, uint16_t instanceId, uint32_t requestId, Metadata &metadata, bool success ) {
	this->type = success ? SERVER_PEER_EVENT_TYPE_FORWARD_CHUNK_RESPONSE_SUCCESS : SERVER_PEER_EVENT_TYPE_FORWARD_CHUNK_RESPONSE_FAILURE;
	this->set( instanceId, requestId, socket );
	this->message.chunk.metadata = metadata;
	this->message.chunk.chunk = 0;
}

void ServerPeerEvent::reqSealChunk( Chunk *chunk ) {
	this->type = SERVER_PEER_EVENT_TYPE_SEAL_CHUNK_REQUEST;
	this->message.chunk.chunk = chunk;
}

void ServerPeerEvent::reqSealChunks( MixedChunkBuffer *chunkBuffer ) {
	this->type = SERVER_PEER_EVENT_TYPE_SEAL_CHUNKS;
	this->message.chunkBuffer = chunkBuffer;
}

void ServerPeerEvent::resSealChunk( ServerPeerSocket *socket, uint16_t instanceId, uint32_t requestId, Metadata &metadata, bool success ) {
	this->type = success ? SERVER_PEER_EVENT_TYPE_SEAL_CHUNK_RESPONSE_SUCCESS : SERVER_PEER_EVENT_TYPE_SEAL_CHUNK_RESPONSE_FAILURE;
	this->set( instanceId, requestId, socket );
	this->message.chunk.metadata = metadata;
	this->message.chunk.chunk = 0;
}

void ServerPeerEvent::resUnsealedKeys( ServerPeerSocket *socket, uint16_t instanceId, uint32_t requestId, struct BatchKeyValueHeader &header, bool success ) {
	this->type = success ? SERVER_PEER_EVENT_TYPE_UNSEALED_KEYS_RESPONSE_SUCCESS : SERVER_PEER_EVENT_TYPE_UNSEALED_KEYS_RESPONSE_FAILURE;
	this->set( instanceId, requestId, socket );
	this->message.unsealedKeys.header = header;
}

void ServerPeerEvent::defer( ServerPeerSocket *socket, uint16_t instanceId, uint32_t requestId, uint8_t opcode, char *buf, size_t size ) {
	this->type = SERVER_PEER_EVENT_TYPE_DEFERRED;
	this->set( instanceId, requestId, socket );
	this->message.defer.opcode = opcode;
	this->message.defer.buf = new char[ size ];
	this->message.defer.size = size;
	memcpy( this->message.defer.buf, buf, size );
}

void ServerPeerEvent::send( ServerPeerSocket *socket, Packet *packet ) {
	this->type = SERVER_PEER_EVENT_TYPE_SEND;
	this->socket = socket;
	this->message.send.packet = packet;
}

void ServerPeerEvent::pending( ServerPeerSocket *socket ) {
	this->type = SERVER_PEER_EVENT_TYPE_PENDING;
	this->socket = socket;
}

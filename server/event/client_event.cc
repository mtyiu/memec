#include "client_event.hh"

void ClientEvent::resRegister( ClientSocket *socket, uint16_t instanceId, uint32_t requestId, bool success ) {
	this->type = success ? CLIENT_EVENT_TYPE_REGISTER_RESPONSE_SUCCESS : CLIENT_EVENT_TYPE_REGISTER_RESPONSE_FAILURE;
	this->instanceId = instanceId;
	this->requestId = requestId;
	this->socket = socket;
}

void ClientEvent::resGet( ClientSocket *socket, uint16_t instanceId, uint32_t requestId, KeyValue &keyValue, bool isDegraded ) {
	this->type = CLIENT_EVENT_TYPE_GET_RESPONSE_SUCCESS;
	this->instanceId = instanceId;
	this->requestId = requestId;
	this->isDegraded = isDegraded;
	this->socket = socket;
	this->message.keyValue = keyValue;
}

void ClientEvent::resGet( ClientSocket *socket, uint16_t instanceId, uint32_t requestId, Key &key, bool isDegraded ) {
	this->type = CLIENT_EVENT_TYPE_GET_RESPONSE_FAILURE;
	this->instanceId = instanceId;
	this->requestId = requestId;
	this->isDegraded = isDegraded;
	this->socket = socket;
	this->message.key = key;
}

void ClientEvent::resSet( ClientSocket *socket, uint16_t instanceId, uint32_t requestId, Key &key, bool success ) {
	this->type = success ? CLIENT_EVENT_TYPE_SET_RESPONSE_SUCCESS_PARITY : CLIENT_EVENT_TYPE_SET_RESPONSE_FAILURE;
	this->instanceId = instanceId;
	this->requestId = requestId;
	this->socket = socket;
	this->message.set.key = key;
}

void ClientEvent::resSet( ClientSocket *socket, uint16_t instanceId, uint32_t requestId, uint32_t timestamp, uint32_t listId, uint32_t stripeId, uint32_t chunkId, bool isSealed, uint32_t sealedListId, uint32_t sealedStripeId, uint32_t sealedChunkId, Key &key ) {
	this->type = CLIENT_EVENT_TYPE_SET_RESPONSE_SUCCESS_DATA;
	this->instanceId = instanceId;
	this->requestId = requestId;
	this->socket = socket;
	this->message.set.key = key;
	this->message.set.timestamp = timestamp;
	this->message.set.listId = listId;
	this->message.set.stripeId = stripeId;
	this->message.set.chunkId = chunkId;
	this->message.set.isSealed = isSealed;
	this->message.set.sealedListId = sealedListId;
	this->message.set.sealedStripeId = sealedStripeId;
	this->message.set.sealedChunkId = sealedChunkId;
}

void ClientEvent::resRemappingSet(
	ClientSocket *socket, uint16_t instanceId, uint32_t requestId, bool success,
	Key &key, uint32_t listId, uint32_t chunkId,
	uint32_t *original, uint32_t *remapped, uint32_t remappedCount,
	bool needsFree
) {
	this->type = success ? CLIENT_EVENT_TYPE_REMAPPING_SET_RESPONSE_SUCCESS : CLIENT_EVENT_TYPE_REMAPPING_SET_RESPONSE_FAILURE;
	this->instanceId = instanceId;
	this->requestId = requestId;
	this->needsFree = needsFree;
	this->socket = socket;
	this->message.remap.key = key;
	this->message.remap.listId = listId;
	this->message.remap.chunkId = chunkId;
	this->message.remap.original = original;
	this->message.remap.remapped = remapped;
	this->message.remap.remappedCount = remappedCount;
}

void ClientEvent::resUpdate( ClientSocket *socket, uint16_t instanceId, uint32_t requestId, Key &key, uint32_t valueUpdateOffset, uint32_t valueUpdateSize, bool success, bool needsFree, bool isDegraded ) {
	this->type = success ? CLIENT_EVENT_TYPE_UPDATE_RESPONSE_SUCCESS : CLIENT_EVENT_TYPE_UPDATE_RESPONSE_FAILURE;
	this->instanceId = instanceId;
	this->requestId = requestId;
	this->needsFree = needsFree;
	this->isDegraded = isDegraded;
	this->socket = socket;
	this->message.keyValueUpdate.key = key;
	this->message.keyValueUpdate.valueUpdateOffset = valueUpdateOffset;
	this->message.keyValueUpdate.valueUpdateSize = valueUpdateSize;
}

void ClientEvent::resDelete( ClientSocket *socket, uint16_t instanceId, uint32_t requestId, uint32_t timestamp, uint32_t listId, uint32_t stripeId, uint32_t chunkId, Key &key, bool needsFree, bool isDegraded ) {
	this->type = CLIENT_EVENT_TYPE_DELETE_RESPONSE_SUCCESS;
	this->instanceId = instanceId;
	this->requestId = requestId;
	this->needsFree = needsFree;
	this->isDegraded = isDegraded;
	this->socket = socket;
	this->message.del.key = key;
	this->message.del.timestamp = timestamp;
	this->message.del.listId = listId;
	this->message.del.stripeId = stripeId;
	this->message.del.chunkId = chunkId;
}

void ClientEvent::resDelete( ClientSocket *socket, uint16_t instanceId, uint32_t requestId, Key &key, bool needsFree, bool isDegraded ) {
	this->type =  CLIENT_EVENT_TYPE_DELETE_RESPONSE_FAILURE;
	this->instanceId = instanceId;
	this->requestId = requestId;
	this->needsFree = needsFree;
	this->isDegraded = isDegraded;
	this->socket = socket;
	this->message.del.key = key;
}

void ClientEvent::resRevertDelta( ClientSocket *socket, uint16_t instanceId, uint32_t requestId, bool success, std::vector<uint32_t> timestamps, std::vector<Key> requests, uint16_t dataServerId ) {
	this->type = success ? CLIENT_EVENT_TYPE_REVERT_DELTA_SUCCESS : CLIENT_EVENT_TYPE_REVERT_DELTA_FAILURE;
	this->instanceId = instanceId;
	this->requestId = requestId;
	this->socket = socket;
	this->message.revert.timestamps = timestamps.empty() ? 0 : new std::vector<uint32_t>( timestamps );
	this->message.revert.requests = requests.empty() ? 0 : new std::vector<Key>( requests );
	this->message.revert.targetId = dataServerId;
}

void ClientEvent::resAckParityDelta( ClientSocket *socket, uint16_t instanceId, uint32_t requestId, std::vector<uint32_t> timestamps , uint16_t dataServerId ) {
	this->type = CLIENT_EVENT_TYPE_ACK_PARITY_BACKUP;
	this->instanceId = instanceId;
	this->requestId = requestId;
	this->socket = socket;
	this->message.revert.timestamps = timestamps.empty() ? 0 : new std::vector<uint32_t>( timestamps );
	this->message.revert.targetId = dataServerId;
}

void ClientEvent::ackMetadata( ClientSocket	*socket, uint16_t instanceId, uint32_t requestId, uint32_t fromTimestamp, uint32_t toTimestamp ) {
	this->type = CLIENT_EVENT_TYPE_ACK_METADATA;
	this->instanceId = instanceId;
	this->requestId = requestId;
	this->socket = socket;
	this->message.ack.fromTimestamp = fromTimestamp;
	this->message.ack.toTimestamp = toTimestamp;
}

void ClientEvent::pending( ClientSocket *socket ) {
	this->type = CLIENT_EVENT_TYPE_PENDING;
	this->socket = socket;
}

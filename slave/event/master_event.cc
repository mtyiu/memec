#include "master_event.hh"

void MasterEvent::resRegister( MasterSocket *socket, uint16_t instanceId, uint32_t requestId, bool success ) {
	this->type = success ? MASTER_EVENT_TYPE_REGISTER_RESPONSE_SUCCESS : MASTER_EVENT_TYPE_REGISTER_RESPONSE_FAILURE;
	this->instanceId = instanceId;
	this->requestId = requestId;
	this->socket = socket;
}

void MasterEvent::resGet( MasterSocket *socket, uint16_t instanceId, uint32_t requestId, KeyValue &keyValue, bool isDegraded ) {
	this->type = MASTER_EVENT_TYPE_GET_RESPONSE_SUCCESS;
	this->instanceId = instanceId;
	this->requestId = requestId;
	this->isDegraded = isDegraded;
	this->socket = socket;
	this->message.keyValue = keyValue;
}

void MasterEvent::resGet( MasterSocket *socket, uint16_t instanceId, uint32_t requestId, Key &key, bool isDegraded ) {
	this->type = MASTER_EVENT_TYPE_GET_RESPONSE_FAILURE;
	this->instanceId = instanceId;
	this->requestId = requestId;
	this->isDegraded = isDegraded;
	this->socket = socket;
	this->message.key = key;
}

void MasterEvent::resSet( MasterSocket *socket, uint16_t instanceId, uint32_t requestId, Key &key, bool success ) {
	this->type = success ? MASTER_EVENT_TYPE_SET_RESPONSE_SUCCESS_PARITY : MASTER_EVENT_TYPE_SET_RESPONSE_FAILURE;
	this->instanceId = instanceId;
	this->requestId = requestId;
	this->socket = socket;
	this->message.set.key = key;
}

void MasterEvent::resSet( MasterSocket *socket, uint16_t instanceId, uint32_t requestId, uint32_t timestamp, uint32_t listId, uint32_t stripeId, uint32_t chunkId, bool isSealed, uint32_t sealedListId, uint32_t sealedStripeId, uint32_t sealedChunkId, Key &key ) {
	this->type = MASTER_EVENT_TYPE_SET_RESPONSE_SUCCESS_DATA;
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

void MasterEvent::resRemappingSet( MasterSocket *socket, uint16_t instanceId, uint32_t requestId, Key &key, uint32_t listId, uint32_t chunkId, bool success, bool needsFree, uint32_t sockfd, bool remapped ) {
	this->type = success ? MASTER_EVENT_TYPE_REMAPPING_SET_RESPONSE_SUCCESS : MASTER_EVENT_TYPE_REMAPPING_SET_RESPONSE_FAILURE;
	this->instanceId = instanceId;
	this->requestId = requestId;
	this->needsFree = needsFree;
	this->socket = socket;
	this->message.remap.key = key;
	this->message.remap.listId = listId;
	this->message.remap.chunkId = chunkId;
	this->message.remap.sockfd = sockfd;
	this->message.remap.isRemapped = remapped;
}

void MasterEvent::resUpdate( MasterSocket *socket, uint16_t instanceId, uint32_t requestId, Key &key, uint32_t valueUpdateOffset, uint32_t valueUpdateSize, bool success, bool needsFree, bool isDegraded ) {
	this->type = success ? MASTER_EVENT_TYPE_UPDATE_RESPONSE_SUCCESS : MASTER_EVENT_TYPE_UPDATE_RESPONSE_FAILURE;
	this->instanceId = instanceId;
	this->requestId = requestId;
	this->needsFree = needsFree;
	this->isDegraded = isDegraded;
	this->socket = socket;
	this->message.keyValueUpdate.key = key;
	this->message.keyValueUpdate.valueUpdateOffset = valueUpdateOffset;
	this->message.keyValueUpdate.valueUpdateSize = valueUpdateSize;
}

void MasterEvent::resDelete( MasterSocket *socket, uint16_t instanceId, uint32_t requestId, uint32_t timestamp, uint32_t listId, uint32_t stripeId, uint32_t chunkId, Key &key, bool needsFree, bool isDegraded ) {
	this->type = MASTER_EVENT_TYPE_DELETE_RESPONSE_SUCCESS;
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

void MasterEvent::resDelete( MasterSocket *socket, uint16_t instanceId, uint32_t requestId, Key &key, bool needsFree, bool isDegraded ) {
	this->type =  MASTER_EVENT_TYPE_DELETE_RESPONSE_FAILURE;
	this->instanceId = instanceId;
	this->requestId = requestId;
	this->needsFree = needsFree;
	this->isDegraded = isDegraded;
	this->socket = socket;
	this->message.del.key = key;
}

void MasterEvent::ackMetadata( MasterSocket	*socket, uint16_t instanceId, uint32_t requestId, uint32_t fromTimestamp, uint32_t toTimestamp ) {
	this->type = MASTER_EVENT_TYPE_ACK_METADATA;
	this->instanceId = instanceId;
	this->requestId = requestId;
	this->socket = socket;
	this->message.ack.fromTimestamp = fromTimestamp;
	this->message.ack.toTimestamp = toTimestamp;
}

void MasterEvent::pending( MasterSocket *socket ) {
	this->type = MASTER_EVENT_TYPE_PENDING;
	this->socket = socket;
}

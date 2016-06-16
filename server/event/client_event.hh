#ifndef __SERVER_EVENT_CLIENT_EVENT_HH__
#define __SERVER_EVENT_CLIENT_EVENT_HH__

#include "../socket/client_socket.hh"
#include "../../common/ds/key.hh"
#include "../../common/ds/key_value.hh"
#include "../../common/ds/metadata.hh"
#include "../../common/event/event.hh"

enum ClientEventType {
	CLIENT_EVENT_TYPE_UNDEFINED,
	// Register
	CLIENT_EVENT_TYPE_REGISTER_RESPONSE_SUCCESS,
	CLIENT_EVENT_TYPE_REGISTER_RESPONSE_FAILURE,
	// GET
	CLIENT_EVENT_TYPE_GET_RESPONSE_SUCCESS,
	CLIENT_EVENT_TYPE_GET_RESPONSE_FAILURE,
	// SET
	CLIENT_EVENT_TYPE_SET_RESPONSE_SUCCESS_DATA,
	CLIENT_EVENT_TYPE_SET_RESPONSE_SUCCESS_PARITY,
	CLIENT_EVENT_TYPE_SET_RESPONSE_FAILURE,
	// DEGRADED_SET
	CLIENT_EVENT_TYPE_DEGRADED_SET_RESPONSE_SUCCESS,
	CLIENT_EVENT_TYPE_DEGRADED_SET_RESPONSE_FAILURE,
	// UPDATE
	CLIENT_EVENT_TYPE_UPDATE_RESPONSE_SUCCESS,
	CLIENT_EVENT_TYPE_UPDATE_RESPONSE_FAILURE,
	// DELETE
	CLIENT_EVENT_TYPE_DELETE_RESPONSE_SUCCESS,
	CLIENT_EVENT_TYPE_DELETE_RESPONSE_FAILURE,
	// ACK
	CLIENT_EVENT_TYPE_ACK_METADATA,
	CLIENT_EVENT_TYPE_ACK_PARITY_BACKUP,
	// FAULT TOLERANCE
	CLIENT_EVENT_TYPE_REVERT_DELTA_SUCCESS,
	CLIENT_EVENT_TYPE_REVERT_DELTA_FAILURE,
	// Pending
	CLIENT_EVENT_TYPE_PENDING
};

class ClientEvent : public Event<ClientSocket> {
public:
	ClientEventType type;
	bool needsFree;
	bool isDegraded;
	uint32_t timestamp;
	union {
		Key key;
		KeyValue keyValue;
		struct {
			uint32_t timestamp;
			uint32_t listId;
			uint32_t stripeId;
			uint32_t chunkId;
			uint8_t sealedCount;
			Metadata sealed[ 2 ];
			Key key;
		} set;
		struct {
			uint32_t timestamp;
			uint32_t listId;
			uint32_t stripeId;
			uint32_t chunkId;
			Key key;
		} del;
		struct {
			// key-value update
			Key key;
			uint32_t valueUpdateOffset;
			uint32_t valueUpdateSize;
		} keyValueUpdate;
		struct {
			Key key;
			uint32_t listId;
			uint32_t chunkId;
			uint32_t *original;
			uint32_t *remapped;
			uint32_t remappedCount;
		} remap;
		struct {
			uint32_t fromTimestamp;
			uint32_t toTimestamp;
		} ack;
		struct {
			std::vector<uint32_t> *timestamps;
			std::vector<Key> *requests;
			uint16_t targetId;
		} revert;
	} message;

	// Register
	inline void resRegister( ClientSocket *socket, uint16_t instanceId, uint32_t requestId, bool success = true ) {
		this->type = success ? CLIENT_EVENT_TYPE_REGISTER_RESPONSE_SUCCESS : CLIENT_EVENT_TYPE_REGISTER_RESPONSE_FAILURE;
		this->set( instanceId, requestId, socket );
	}

	// GET
	inline void resGet(
		ClientSocket *socket, uint16_t instanceId, uint32_t requestId,
		KeyValue &keyValue, bool isDegraded
	) {
		this->type = CLIENT_EVENT_TYPE_GET_RESPONSE_SUCCESS;
		this->set( instanceId, requestId, socket );
		this->isDegraded = isDegraded;
		this->message.keyValue = keyValue;
	}

	inline void resGet(
		ClientSocket *socket, uint16_t instanceId, uint32_t requestId,
		Key &key, bool isDegraded
	) {
		this->type = CLIENT_EVENT_TYPE_GET_RESPONSE_FAILURE;
		this->set( instanceId, requestId, socket );
		this->isDegraded = isDegraded;
		this->message.key = key;
	}

	// SET
	inline void resSet(
		ClientSocket *socket, uint16_t instanceId, uint32_t requestId, uint32_t timestamp,
		uint32_t listId, uint32_t stripeId, uint32_t chunkId,
		uint8_t sealedCount, Metadata *sealed,
		Key &key
	) {
		this->type = CLIENT_EVENT_TYPE_SET_RESPONSE_SUCCESS_DATA;
		this->set( instanceId, requestId, socket );
		this->message.set.timestamp = timestamp;
		this->message.set.listId = listId;
		this->message.set.stripeId = stripeId;
		this->message.set.chunkId = chunkId;
		this->message.set.sealedCount = sealedCount;
		if ( sealedCount ) {
			for ( int i = 0; i < sealedCount; i++ ) {
				this->message.set.sealed[ i ] = sealed[ i ];
			}
		}
		this->message.set.key = key;
	}

	inline void resSet(
		ClientSocket *socket, uint16_t instanceId, uint32_t requestId,
		Key &key, bool success
	) {
		this->type = success ? CLIENT_EVENT_TYPE_SET_RESPONSE_SUCCESS_PARITY : CLIENT_EVENT_TYPE_SET_RESPONSE_FAILURE;
		this->set( instanceId, requestId, socket );
		this->message.set.key = key;
	}

	// DEGRADED_SET
	inline void resDegradedSet(
		ClientSocket *socket, uint16_t instanceId, uint32_t requestId, bool success,
		Key &key, uint32_t listId, uint32_t chunkId,
		uint32_t *original, uint32_t *remapped, uint32_t remappedCount,
		bool needsFree
	) {
		this->type = success ? CLIENT_EVENT_TYPE_DEGRADED_SET_RESPONSE_SUCCESS : CLIENT_EVENT_TYPE_DEGRADED_SET_RESPONSE_FAILURE;
		this->set( instanceId, requestId, socket );
		this->needsFree = needsFree;
		this->message.remap = {
			.key = key,
			.listId = listId,
			.chunkId = chunkId,
			.original = original,
			.remapped = remapped,
			.remappedCount = remappedCount
		};
	}

	// UPDATE
	inline void resUpdate( ClientSocket *socket, uint16_t instanceId, uint32_t requestId, Key &key, uint32_t valueUpdateOffset, uint32_t valueUpdateSize, bool success, bool needsFree, bool isDegraded ) {
		this->type = success ? CLIENT_EVENT_TYPE_UPDATE_RESPONSE_SUCCESS : CLIENT_EVENT_TYPE_UPDATE_RESPONSE_FAILURE;
		this->set( instanceId, requestId, socket );
		this->needsFree = needsFree;
		this->isDegraded = isDegraded;
		this->message.keyValueUpdate = {
			.key = key,
			.valueUpdateOffset = valueUpdateOffset,
			.valueUpdateSize = valueUpdateSize
		};
	}

	// DELETE
	inline void resDelete( ClientSocket *socket, uint16_t instanceId, uint32_t requestId, uint32_t timestamp, uint32_t listId, uint32_t stripeId, uint32_t chunkId, Key &key, bool needsFree, bool isDegraded ) {
		this->type = CLIENT_EVENT_TYPE_DELETE_RESPONSE_SUCCESS;
		this->set( instanceId, requestId, socket );
		this->needsFree = needsFree;
		this->isDegraded = isDegraded;
		this->message.del = {
			.timestamp = timestamp,
			.listId = listId,
			.stripeId = stripeId,
			.chunkId = chunkId,
			.key = key
		};
	}

	inline void resDelete( ClientSocket *socket, uint16_t instanceId, uint32_t requestId, Key &key, bool needsFree, bool isDegraded ) {
		this->type =  CLIENT_EVENT_TYPE_DELETE_RESPONSE_FAILURE;
		this->set( instanceId, requestId, socket );
		this->needsFree = needsFree;
		this->isDegraded = isDegraded;
		this->message.del.key = key;
	}

	// FAULT TOLERANCE
	inline void resAckParityDelta( ClientSocket *socket, uint16_t instanceId, uint32_t requestId, std::vector<uint32_t> timestamps, uint16_t dataServerId ) {
		this->type = CLIENT_EVENT_TYPE_ACK_PARITY_BACKUP;
		this->set( instanceId, requestId, socket );
		this->message.revert.timestamps = timestamps.empty() ? 0 : new std::vector<uint32_t>( timestamps );
		this->message.revert.targetId = dataServerId;
	}

	inline void resRevertDelta( ClientSocket *socket, uint16_t instanceId, uint32_t requestId, bool success, std::vector<uint32_t> timestamps, std::vector<Key> requests, uint16_t dataServerId ) {
		this->type = success ? CLIENT_EVENT_TYPE_REVERT_DELTA_SUCCESS : CLIENT_EVENT_TYPE_REVERT_DELTA_FAILURE;
		this->set( instanceId, requestId, socket );
		this->message.revert = {
			.timestamps = timestamps.empty() ? 0 : new std::vector<uint32_t>( timestamps ),
			.requests = requests.empty() ? 0 : new std::vector<Key>( requests ),
			.targetId = dataServerId
		};
	}

	// ACK
	inline void ackMetadata( ClientSocket *socket, uint16_t instanceId, uint32_t requestId, uint32_t fromTimestamp, uint32_t toTimestamp ) {
		this->type = CLIENT_EVENT_TYPE_ACK_METADATA;
		this->set( instanceId, requestId, socket );
		this->message.ack = {
			.fromTimestamp = fromTimestamp,
			.toTimestamp = toTimestamp
		};
	}

	// Pending
	inline void pending( ClientSocket *socket ) {
		this->type = CLIENT_EVENT_TYPE_PENDING;
		this->socket = socket;
	}
};

#endif

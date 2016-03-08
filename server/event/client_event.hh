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
	// REMAPPING_SET
	CLIENT_EVENT_TYPE_REMAPPING_SET_RESPONSE_SUCCESS,
	CLIENT_EVENT_TYPE_REMAPPING_SET_RESPONSE_FAILURE,
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

class ClientEvent : public Event {
public:
	ClientEventType type;
	uint16_t instanceId;
	uint32_t requestId;
	bool needsFree;
	bool isDegraded;
	ClientSocket *socket;
	uint32_t timestamp;
	union {
		Key key;
		KeyValue keyValue;
		struct {
			uint32_t timestamp;
			uint32_t listId;
			uint32_t stripeId;
			uint32_t chunkId;
			bool isSealed;
			uint32_t sealedListId;
			uint32_t sealedStripeId;
			uint32_t sealedChunkId;
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
	void resRegister( ClientSocket *socket, uint16_t instanceId, uint32_t requestId, bool success = true );
	// GET
	void resGet( ClientSocket *socket, uint16_t instanceId, uint32_t requestId, KeyValue &keyValue, bool isDegraded );
	void resGet( ClientSocket *socket, uint16_t instanceId, uint32_t requestId, Key &key, bool isDegraded );
	// SET
	void resSet( ClientSocket *socket, uint16_t instanceId, uint32_t requestId, uint32_t timestamp, uint32_t listId, uint32_t stripeId, uint32_t chunkId, bool isSealed, uint32_t sealedListId, uint32_t sealedStripeId, uint32_t sealedChunkId, Key &key );
	void resSet( ClientSocket *socket, uint16_t instanceId, uint32_t requestId, Key &key, bool success );
	// REMAPPING_SET
	void resRemappingSet(
		ClientSocket *socket, uint16_t instanceId, uint32_t requestId, bool success,
		Key &key, uint32_t listId, uint32_t chunkId,
		uint32_t *original, uint32_t *remapped, uint32_t remappedCount,
		bool needsFree
	);
	// UPDATE
	void resUpdate( ClientSocket *socket, uint16_t instanceId, uint32_t requestId, Key &key, uint32_t valueUpdateOffset, uint32_t valueUpdateSize, bool success, bool needsFree, bool isDegraded );
	// DELETE
	void resDelete( ClientSocket *socket, uint16_t instanceId, uint32_t requestId, uint32_t timestamp, uint32_t listId, uint32_t stripeId, uint32_t chunkId, Key &key, bool needsFree, bool isDegraded );
	void resDelete( ClientSocket *socket, uint16_t instanceId, uint32_t requestId, Key &key, bool needsFree, bool isDegraded );
	// FAULT TOLERANCE
	void resAckParityDelta( ClientSocket *socket, uint16_t instanceId, uint32_t requestId, std::vector<uint32_t> timestamps, uint16_t dataServerId );
	void resRevertDelta( ClientSocket *socket, uint16_t instanceId, uint32_t requestId, bool success, std::vector<uint32_t> timestamps, std::vector<Key> requests, uint16_t dataServerId );
	// ACK
	void ackMetadata( ClientSocket *socket, uint16_t instanceId, uint32_t requestId, uint32_t fromTimestamp, uint32_t toTimestamp );

	// Pending
	void pending( ClientSocket *socket );
};

#endif

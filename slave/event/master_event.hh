#ifndef __SLAVE_EVENT_MASTER_EVENT_HH__
#define __SLAVE_EVENT_MASTER_EVENT_HH__

#include "../socket/master_socket.hh"
#include "../../common/ds/key.hh"
#include "../../common/ds/key_value.hh"
#include "../../common/ds/metadata.hh"
#include "../../common/event/event.hh"

enum MasterEventType {
	MASTER_EVENT_TYPE_UNDEFINED,
	// Register
	MASTER_EVENT_TYPE_REGISTER_RESPONSE_SUCCESS,
	MASTER_EVENT_TYPE_REGISTER_RESPONSE_FAILURE,
	// GET
	MASTER_EVENT_TYPE_GET_RESPONSE_SUCCESS,
	MASTER_EVENT_TYPE_GET_RESPONSE_FAILURE,
	// SET
	MASTER_EVENT_TYPE_SET_RESPONSE_SUCCESS_DATA,
	MASTER_EVENT_TYPE_SET_RESPONSE_SUCCESS_PARITY,
	MASTER_EVENT_TYPE_SET_RESPONSE_FAILURE,
	// REMAPPING_SET
	MASTER_EVENT_TYPE_REMAPPING_SET_RESPONSE_SUCCESS,
	MASTER_EVENT_TYPE_REMAPPING_SET_RESPONSE_FAILURE,
	// UPDATE
	MASTER_EVENT_TYPE_UPDATE_RESPONSE_SUCCESS,
	MASTER_EVENT_TYPE_UPDATE_RESPONSE_FAILURE,
	// DELETE
	MASTER_EVENT_TYPE_DELETE_RESPONSE_SUCCESS,
	MASTER_EVENT_TYPE_DELETE_RESPONSE_FAILURE,
	// ACK
	MASTER_EVENT_TYPE_ACK_METADATA,
	// Pending
	MASTER_EVENT_TYPE_PENDING
};

class MasterEvent : public Event {
public:
	MasterEventType type;
	uint16_t instanceId;
	uint32_t requestId;
	bool needsFree;
	bool isDegraded;
	MasterSocket *socket;
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
	} message;

	// Register
	void resRegister( MasterSocket *socket, uint16_t instanceId, uint32_t requestId, bool success = true );
	// GET
	void resGet( MasterSocket *socket, uint16_t instanceId, uint32_t requestId, KeyValue &keyValue, bool isDegraded );
	void resGet( MasterSocket *socket, uint16_t instanceId, uint32_t requestId, Key &key, bool isDegraded );
	// SET
	void resSet( MasterSocket *socket, uint16_t instanceId, uint32_t requestId, uint32_t timestamp, uint32_t listId, uint32_t stripeId, uint32_t chunkId, bool isSealed, uint32_t sealedListId, uint32_t sealedStripeId, uint32_t sealedChunkId, Key &key );
	void resSet( MasterSocket *socket, uint16_t instanceId, uint32_t requestId, Key &key, bool success );
	// REMAPPING_SET
	void resRemappingSet(
		MasterSocket *socket, uint16_t instanceId, uint32_t requestId, bool success,
		Key &key, uint32_t listId, uint32_t chunkId,
		uint32_t *original, uint32_t *remapped, uint32_t remappedCount,
		bool needsFree
	);
	// UPDATE
	void resUpdate( MasterSocket *socket, uint16_t instanceId, uint32_t requestId, Key &key, uint32_t valueUpdateOffset, uint32_t valueUpdateSize, bool success, bool needsFree, bool isDegraded );
	// DELETE
	void resDelete( MasterSocket *socket, uint16_t instanceId, uint32_t requestId, uint32_t timestamp, uint32_t listId, uint32_t stripeId, uint32_t chunkId, Key &key, bool needsFree, bool isDegraded );
	void resDelete( MasterSocket *socket, uint16_t instanceId, uint32_t requestId, Key &key, bool needsFree, bool isDegraded );
	// ACK
	void ackMetadata( MasterSocket *socket, uint16_t instanceId, uint32_t requestId, uint32_t fromTimestamp, uint32_t toTimestamp );
	// Pending
	void pending( MasterSocket *socket );
};

#endif

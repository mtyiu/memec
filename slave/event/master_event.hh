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
	MASTER_EVENT_TYPE_SET_RESPONSE_SUCCESS,
	MASTER_EVENT_TYPE_SET_RESPONSE_FAILURE,
	// REMAPPING_SET_LOCK
	MASTER_EVENT_TYPE_REMAPPING_SET_LOCK_RESPONSE_SUCCESS,
	MASTER_EVENT_TYPE_REMAPPING_SET_LOCK_RESPONSE_FAILURE,
	// REMAPPING_SET
	MASTER_EVENT_TYPE_REMAPPING_SET_RESPONSE_SUCCESS,
	MASTER_EVENT_TYPE_REMAPPING_SET_RESPONSE_FAILURE,
	// UPDATE
	MASTER_EVENT_TYPE_UPDATE_RESPONSE_SUCCESS,
	MASTER_EVENT_TYPE_UPDATE_RESPONSE_FAILURE,
	// DELETE
	MASTER_EVENT_TYPE_DELETE_RESPONSE_SUCCESS,
	MASTER_EVENT_TYPE_DELETE_RESPONSE_FAILURE,
	// REDIRECT
	MASTER_EVENT_TYPE_REDIRECT_RESPONSE,
	// Pending
	MASTER_EVENT_TYPE_PENDING
};

class MasterEvent : public Event {
public:
	MasterEventType type;
	uint32_t id;
	bool needsFree;
	bool isDegraded;
	MasterSocket *socket;
	union {
		Key key;
		KeyValue keyValue;
		struct {
			// key-value update
			Key key;
			uint32_t valueUpdateOffset;
			uint32_t valueUpdateSize;
		} keyValueUpdate;
		struct {
			Key key;
			uint8_t opcode;
			uint32_t listId, chunkId;
		} remap;
	} message;

	// Register
	void resRegister( MasterSocket *socket, uint32_t id, bool success = true );
	// GET
	void resGet( MasterSocket *socket, uint32_t id, KeyValue &keyValue, bool isDegraded );
	void resGet( MasterSocket *socket, uint32_t id, Key &key, bool isDegraded );
	// SET
	void resSet( MasterSocket *socket, uint32_t id, Key &key, bool success );
	// REMAPPING_SET_LOCK
	void resRemappingSetLock( MasterSocket *socket, uint32_t id, Key &key, RemappingRecord &remappingRecord, bool success );
	// REMAPPING_SET
	void resRemappingSet( MasterSocket *socket, uint32_t id, Key &key, uint32_t listId, uint32_t chunkId, bool success, bool needsFree );
	// UPDATE
	void resUpdate( MasterSocket *socket, uint32_t id, Key &key, uint32_t valueUpdateOffset, uint32_t valueUpdateSize, bool success, bool needsFree, bool isDegraded );
	// DELETE
	void resDelete( MasterSocket *socket, uint32_t id, Key &key, bool success, bool needsFree, bool isDegraded );
	// Redirect
	void resRedirect( MasterSocket *socket, uint32_t id, uint8_t opcode, Key &key, RemappingRecord record );
	// Pending
	void pending( MasterSocket *socket );
};

#endif

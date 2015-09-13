#ifndef __SLAVE_EVENT_MASTER_EVENT_HH__
#define __SLAVE_EVENT_MASTER_EVENT_HH__

#include "../socket/master_socket.hh"
#include "../../common/ds/key.hh"
#include "../../common/ds/key_value.hh"
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
	// UPDATE
	MASTER_EVENT_TYPE_UPDATE_RESPONSE_SUCCESS,
	MASTER_EVENT_TYPE_UPDATE_RESPONSE_FAILURE,
	// DELETE
	MASTER_EVENT_TYPE_DELETE_RESPONSE_SUCCESS,
	MASTER_EVENT_TYPE_DELETE_RESPONSE_FAILURE,
	// Pending
	MASTER_EVENT_TYPE_PENDING
};

class MasterEvent : public Event {
public:
	MasterEventType type;
	bool needsFree;
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
	} message;

	// Register
	void resRegister( MasterSocket *socket, bool success = true );
	// GET
	void resGet( MasterSocket *socket, KeyValue &keyValue );
	void resGet( MasterSocket *socket, Key &key );
	// SET
	void resSet( MasterSocket *socket, Key &key, bool success );
	// UPDATE
	void resUpdate( MasterSocket *socket, Key &key, uint32_t valueUpdateOffset, uint32_t valueUpdateSize, bool success, bool needsFree = true );
	// DELETE
	void resDelete( MasterSocket *socket, Key &key, bool success, bool needsFree = true );
	// Pending
	void pending( MasterSocket *socket );
};

#endif

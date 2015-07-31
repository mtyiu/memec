#ifndef __SLAVE_EVENT_MASTER_EVENT_HH__
#define __SLAVE_EVENT_MASTER_EVENT_HH__

#include "../socket/master_socket.hh"
#include "../../common/ds/key.hh"
#include "../../common/ds/key_value.hh"
#include "../../common/event/event.hh"

enum MasterEventType {
	MASTER_EVENT_TYPE_UNDEFINED,
	MASTER_EVENT_TYPE_REGISTER_RESPONSE_SUCCESS,
	MASTER_EVENT_TYPE_REGISTER_RESPONSE_FAILURE,
	MASTER_EVENT_TYPE_GET_RESPONSE_SUCCESS,
	MASTER_EVENT_TYPE_GET_RESPONSE_FAILURE,
	MASTER_EVENT_TYPE_SET_RESPONSE_SUCCESS,
	MASTER_EVENT_TYPE_SET_RESPONSE_FAILURE,
	MASTER_EVENT_TYPE_PENDING
};

class MasterEvent : public Event {
public:
	MasterEventType type;
	MasterSocket *socket;
	union {
		Key key;
		KeyValue keyValue;
	} message;

	void resRegister( MasterSocket *socket, bool success = true );
	void resGet( MasterSocket *socket, KeyValue &keyValue );
	void resGet( MasterSocket *socket, Key &key );
	void resSet( MasterSocket *socket, Key &key );
	void pending( MasterSocket *socket );
};

#endif

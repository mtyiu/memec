#ifndef __CLIENT_EVENT_APPLICATION_EVENT_HH__
#define __CLIENT_EVENT_APPLICATION_EVENT_HH__

#include "../socket/application_socket.hh"
#include "../../common/ds/key.hh"
#include "../../common/ds/key_value.hh"
#include "../../common/ds/pending.hh"
#include "../../common/event/event.hh"

enum ApplicationEventType {
	APPLICATION_EVENT_TYPE_UNDEFINED,
	APPLICATION_EVENT_TYPE_REGISTER_RESPONSE_SUCCESS,
	APPLICATION_EVENT_TYPE_REGISTER_RESPONSE_FAILURE,
	APPLICATION_EVENT_TYPE_GET_RESPONSE_SUCCESS,
	APPLICATION_EVENT_TYPE_GET_RESPONSE_FAILURE,
	APPLICATION_EVENT_TYPE_SET_RESPONSE_SUCCESS,
	APPLICATION_EVENT_TYPE_SET_RESPONSE_FAILURE,
	APPLICATION_EVENT_TYPE_UPDATE_RESPONSE_SUCCESS,
	APPLICATION_EVENT_TYPE_UPDATE_RESPONSE_FAILURE,
	APPLICATION_EVENT_TYPE_DELETE_RESPONSE_SUCCESS,
	APPLICATION_EVENT_TYPE_DELETE_RESPONSE_FAILURE,
	APPLICATION_EVENT_TYPE_REPLAY_SET,
	APPLICATION_EVENT_TYPE_REPLAY_GET,
	APPLICATION_EVENT_TYPE_REPLAY_UPDATE,
	APPLICATION_EVENT_TYPE_REPLAY_DEL,
	APPLICATION_EVENT_TYPE_PENDING
};

class ApplicationEvent : public Event {
public:
	ApplicationEventType type;
	uint16_t instanceId;
	uint32_t requestId;
	ApplicationSocket *socket;
	bool needsFree;
	union {
		Key key;
		struct {
			bool isKeyValue;
			union {
				Key key;
				KeyValue keyValue;
			} data;
		} set;
		struct {
			uint8_t keySize;
			uint32_t valueSize;
			char *keyStr;
			char *valueStr;
		} get;
		KeyValueUpdate keyValueUpdate;
		union {
			struct {
				KeyValue keyValue;
			} set;
			struct {
				Key key;
			} get;
			struct {
				KeyValueUpdate keyValueUpdate;
			} update;
			struct {
				Key key;
			} del;
		} replay;
	} message;

	void resRegister( ApplicationSocket *socket, uint16_t instanceId, uint32_t requestId, bool success = true );
	void resGet( ApplicationSocket *socket, uint16_t instanceId, uint32_t requestId, uint8_t keySize, uint32_t valueSize, char *keyStr, char *valueStr, bool needsFree = true );
	void resGet( ApplicationSocket *socket, uint16_t instanceId, uint32_t requestId, Key &key, bool needsFree = true );
	void resSet( ApplicationSocket *socket, uint16_t instanceId, uint32_t requestId, KeyValue &keyValue, bool success, bool needsFree = true );
	void resSet( ApplicationSocket *socket, uint16_t instanceId, uint32_t requestId, Key &key, bool success, bool needsFree = true );
	void resUpdate( ApplicationSocket *socket, uint16_t instanceId, uint32_t requestId, KeyValueUpdate &keyValueUpdate, bool success, bool needsFree = true );
	void resDelete( ApplicationSocket *socket, uint16_t instanceId, uint32_t requestId, Key &key, bool success, bool needsFree = true );

	void replaySetRequest( ApplicationSocket *socket, uint16_t instanceId, uint32_t requestId, KeyValue keyValue );
	void replayGetRequest( ApplicationSocket *socket, uint16_t instanceId, uint32_t requestId, Key key );
	void replayUpdateRequest( ApplicationSocket *socket, uint16_t instanceId, uint32_t requestId, KeyValueUpdate keyValueUpdate );
	void replayDeleteRequest( ApplicationSocket *socket, uint16_t instanceId, uint32_t requestId, Key key );

	void pending( ApplicationSocket *socket );
};

#endif

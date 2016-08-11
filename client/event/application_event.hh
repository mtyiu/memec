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
	APPLICATION_EVENT_TYPE_REGISTER_RESPONSE_SUCCESS_WITH_NAMED_PIPE,
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

class ApplicationEvent : public Event<ApplicationSocket> {
public:
	ApplicationEventType type;
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

	inline void resRegister( ApplicationSocket *socket, uint16_t instanceId, uint32_t requestId, bool success = true ) {
		this->type = success ? APPLICATION_EVENT_TYPE_REGISTER_RESPONSE_SUCCESS : APPLICATION_EVENT_TYPE_REGISTER_RESPONSE_FAILURE;
		this->set( instanceId, requestId, socket );
	}

	inline void resRegisterWithNamedPipe( ApplicationSocket *socket, uint16_t instanceId, uint32_t requestId ) {
		this->type = APPLICATION_EVENT_TYPE_REGISTER_RESPONSE_SUCCESS_WITH_NAMED_PIPE;
		this->set( instanceId, requestId, socket );
	}

	inline void resGet(
		ApplicationSocket *socket, uint16_t instanceId, uint32_t requestId,
		uint8_t keySize, uint32_t valueSize, char *keyStr, char *valueStr,
		bool needsFree = true
	) {
		this->type = APPLICATION_EVENT_TYPE_GET_RESPONSE_SUCCESS;
		this->set( instanceId, requestId, socket );
		this->needsFree = needsFree;
		this->message.get = {
			.keySize = keySize,
			.valueSize = valueSize,
			.keyStr = keyStr,
			.valueStr = valueStr
		};
	}

	inline void resGet(
		ApplicationSocket *socket, uint16_t instanceId, uint32_t requestId,
		Key &key, bool needsFree = true
	) {
		this->type = APPLICATION_EVENT_TYPE_GET_RESPONSE_FAILURE;
		this->set( instanceId, requestId, socket );
		this->needsFree = needsFree;
		this->message.key = key;
	}

	inline void resSet(
		ApplicationSocket *socket, uint16_t instanceId, uint32_t requestId,
		KeyValue &keyValue, bool success, bool needsFree = true
	) {
		this->type = success ? APPLICATION_EVENT_TYPE_SET_RESPONSE_SUCCESS : APPLICATION_EVENT_TYPE_SET_RESPONSE_FAILURE;
		this->set( instanceId, requestId, socket );
		this->needsFree = needsFree;
		this->message.set.isKeyValue = true;
		this->message.set.data.keyValue = keyValue;
	}

	inline void resSet(
		ApplicationSocket *socket, uint16_t instanceId, uint32_t requestId,
		Key &key, bool success, bool needsFree = true
	) {
		this->type = success ? APPLICATION_EVENT_TYPE_SET_RESPONSE_SUCCESS : APPLICATION_EVENT_TYPE_SET_RESPONSE_FAILURE;
		this->set( instanceId, requestId, socket );
		this->needsFree = needsFree;
		this->message.set.isKeyValue = false;
		this->message.set.data.key = key;
	}

	inline void resUpdate(
		ApplicationSocket *socket, uint16_t instanceId, uint32_t requestId,
		KeyValueUpdate &keyValueUpdate, bool success, bool needsFree = true
	) {
		this->type = success ? APPLICATION_EVENT_TYPE_UPDATE_RESPONSE_SUCCESS : APPLICATION_EVENT_TYPE_UPDATE_RESPONSE_FAILURE;
		this->needsFree = needsFree;
		this->set( instanceId, requestId, socket );
		this->message.keyValueUpdate = keyValueUpdate;
	}

	inline void resDelete(
		ApplicationSocket *socket, uint16_t instanceId, uint32_t requestId,
		Key &key, bool success, bool needsFree = true
	) {
		this->type = success ? APPLICATION_EVENT_TYPE_DELETE_RESPONSE_SUCCESS : APPLICATION_EVENT_TYPE_DELETE_RESPONSE_FAILURE;
		this->set( instanceId, requestId, socket );
		this->needsFree = needsFree;
		this->message.key = key;
	}

	inline void replaySetRequest( ApplicationSocket *socket, uint16_t instanceId, uint32_t requestId, KeyValue keyValue ) {
		this->type = APPLICATION_EVENT_TYPE_REPLAY_SET;
		this->set( instanceId, requestId, socket );
		this->message.replay.set.keyValue = keyValue;
	}

	inline void replayGetRequest( ApplicationSocket *socket, uint16_t instanceId, uint32_t requestId, Key key ) {
		this->type = APPLICATION_EVENT_TYPE_REPLAY_GET;
		this->set( instanceId, requestId, socket );
		this->message.replay.get.key = key;
	}

	inline void replayUpdateRequest( ApplicationSocket *socket, uint16_t instanceId, uint32_t requestId, KeyValueUpdate keyValueUpdate ) {
		this->type = APPLICATION_EVENT_TYPE_REPLAY_UPDATE;
		this->set( instanceId, requestId, socket );
		this->message.replay.update.keyValueUpdate = keyValueUpdate;
	}

	inline void replayDeleteRequest( ApplicationSocket *socket, uint16_t instanceId, uint32_t requestId, Key key ) {
		this->type = APPLICATION_EVENT_TYPE_REPLAY_DEL;
		this->set( instanceId, requestId, socket );
		this->message.replay.del.key = key;
	}

	inline void pending( ApplicationSocket *socket ) {
		this->type = APPLICATION_EVENT_TYPE_PENDING;
		this->socket = socket;
	}
};

#endif

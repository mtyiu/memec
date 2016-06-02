#ifndef __APPLICATION_EVENT_CLIENT_EVENT_HH__
#define __APPLICATION_EVENT_CLIENT_EVENT_HH__

#include "../socket/client_socket.hh"
#include "../../common/event/event.hh"

enum ClientEventType {
	CLIENT_EVENT_TYPE_UNDEFINED,
	CLIENT_EVENT_TYPE_REGISTER_REQUEST,
	CLIENT_EVENT_TYPE_SET_REQUEST,
	CLIENT_EVENT_TYPE_GET_REQUEST,
	CLIENT_EVENT_TYPE_UPDATE_REQUEST,
	CLIENT_EVENT_TYPE_DELETE_REQUEST,
	CLIENT_EVENT_TYPE_PENDING
};

class ClientEvent : public Event<ClientSocket> {
public:
	ClientEventType type;
	union {
		struct {
			char *key;
			uint32_t keySize;
			int fd;
		} set;
		struct {
			char *key;
			uint32_t keySize;
			int fd;
		} get;
		struct {
			char *key;
			uint32_t keySize;
			uint32_t offset;
			int fd;
		} update;
		struct {
			char *key;
			uint32_t keySize;
		} del;
	} message;

	inline void reqRegister( ClientSocket *socket ) {
		this->type = CLIENT_EVENT_TYPE_REGISTER_REQUEST;
		this->socket = socket;
	}

	inline void reqSet( ClientSocket *socket, char *key, uint32_t keySize, int fd ) {
		this->type = CLIENT_EVENT_TYPE_SET_REQUEST;
		this->socket = socket;
		this->message.set = {
			.key = key,
			.keySize = keySize,
			.fd = fd
		};
	}

	inline void reqGet( ClientSocket *socket, char *key, uint32_t keySize, int fd ) {
		this->type = CLIENT_EVENT_TYPE_GET_REQUEST;
		this->socket = socket;
		this->message.get = {
			.key = key,
			.keySize = keySize,
			.fd = fd
		};
	}

	inline void reqUpdate( ClientSocket *socket, char *key, uint32_t keySize, int fd, uint32_t offset ) {
		this->type = CLIENT_EVENT_TYPE_UPDATE_REQUEST;
		this->socket = socket;
		this->message.update = {
			.key = key,
			.keySize = keySize,
			.offset = offset,
			.fd = fd
		};
	}

	inline void reqDelete( ClientSocket *socket, char *key, uint32_t keySize ) {
		this->type = CLIENT_EVENT_TYPE_DELETE_REQUEST;
		this->socket = socket;
		this->message.del = {
			.key = key,
			.keySize = keySize
		};
	}

	inline void pending( ClientSocket *socket ) {
		this->type = CLIENT_EVENT_TYPE_PENDING;
		this->socket = socket;
	}

};

#endif

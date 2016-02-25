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

class ClientEvent : public Event {
public:
	ClientEventType type;
	ClientSocket *socket;
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

	void reqRegister( ClientSocket *socket );
	void reqSet( ClientSocket *socket, char *key, uint32_t keySize, int fd );
	void reqGet( ClientSocket *socket, char *key, uint32_t keySize, int fd );
	void reqUpdate( ClientSocket *socket, char *key, uint32_t keySize, int fd, uint32_t offset );
	void reqDelete( ClientSocket *socket, char *key, uint32_t keySize );
	void pending( ClientSocket *socket );
};

#endif

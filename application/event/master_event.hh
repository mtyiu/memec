#ifndef __MASTER_EVENT_MASTER_EVENT_HH__
#define __MASTER_EVENT_MASTER_EVENT_HH__

#include "../socket/master_socket.hh"
#include "../../common/event/event.hh"

enum MasterEventType {
	MASTER_EVENT_TYPE_UNDEFINED,
	MASTER_EVENT_TYPE_REGISTER_REQUEST,
	MASTER_EVENT_TYPE_SET_REQUEST,
	MASTER_EVENT_TYPE_GET_REQUEST,
	MASTER_EVENT_TYPE_PENDING
};

class MasterEvent : public Event {
public:
	MasterEventType type;
	MasterSocket *socket;
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
	} message;

	void reqRegister( MasterSocket *socket );
	void reqSet( MasterSocket *socket, char *key, uint32_t keySize, int fd );
	void reqGet( MasterSocket *socket, char *key, uint32_t keySize, int fd );
	void pending( MasterSocket *socket );
};

#endif

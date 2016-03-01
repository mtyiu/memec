#ifndef __SERVER_EVENT_SERVER_EVENT_HH__
#define __SERVER_EVENT_SERVER_EVENT_HH__

#include "../socket/server_socket.hh"
#include "../../common/ds/chunk.hh"
#include "../../common/event/event.hh"

enum ServerEventType {
	SERVER_EVENT_TYPE_UNDEFINED,
	SERVER_EVENT_TYPE_PENDING
};

class ServerEvent : public Event {
public:
	ServerEventType type;
	ServerSocket *socket;

	void pending( ServerSocket *socket );
};

#endif

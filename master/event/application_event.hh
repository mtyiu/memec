#ifndef __MASTER_EVENT_APPLICATION_EVENT_HH__
#define __MASTER_EVENT_APPLICATION_EVENT_HH__

#include "../socket/application_socket.hh"
#include "../../common/event/event.hh"

enum ApplicationEventType {
	APPLICATION_EVENT_TYPE_UNDEFINED,
	APPLICATION_EVENT_TYPE_REGISTER_RESPONSE_SUCCESS,
	APPLICATION_EVENT_TYPE_REGISTER_RESPONSE_FAILURE,
	APPLICATION_EVENT_TYPE_PENDING
};

class ApplicationEvent : public Event {
public:
	ApplicationEventType type;
	ApplicationSocket *socket;

	void resRegister( ApplicationSocket *socket, bool success = true );
	void pending( ApplicationSocket *socket );
};

#endif

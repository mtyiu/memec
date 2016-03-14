#ifndef __CLIENT_EVENT_CLIENT_EVENT_HH__
#define __CLIENT_EVENT_CLIENT_EVENT_HH__

#include "../socket/client_socket.hh"
#include "../socket/application_socket.hh"
#include "../../common/event/event.hh"
#include "../../common/ds/key.hh"
#include "../../common/ds/key_value.hh"
#include "../../common/ds/pending.hh"

enum ClientEventType {
	CLIENT_EVENT_TYPE_UNDEFINED,
	CLIENT_EVENT_TYPE_PENDING
};

class ClientEvent : public Event {
public:
	ClientEventType type;
	ClientSocket *socket;

	void pending( ClientSocket *socket );
};

#endif

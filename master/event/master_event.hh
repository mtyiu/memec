#ifndef __MASTER_EVENT_MASTER_EVENT_HH__
#define __MASTER_EVENT_MASTER_EVENT_HH__

#include "../socket/master_socket.hh"
#include "../../common/event/event.hh"

enum MasterEventType {
	MASTER_EVENT_TYPE_UNDEFINED,
	MASTER_EVENT_TYPE_PENDING
};

class MasterEvent : public Event {
public:
	MasterEventType type;
	MasterSocket *socket;

	void pending( MasterSocket *socket );
};

#endif

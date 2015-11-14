#ifndef __COORDINATOR_EVENT_COORDINATOR_EVENT_HH__
#define __COORDINATOR_EVENT_COORDINATOR_EVENT_HH__

#include <map>
#include "../socket/coordinator_socket.hh"
#include "../../common/ds/sockaddr_in.hh"
#include "../../common/event/event.hh"

enum CoordinatorEventType {
	COORDINATOR_EVENT_TYPE_UNDEFINED,
	COORDINATOR_EVENT_TYPE_SYNC_REMAPPING_RECORDS,
	COORDINATOR_EVENT_TYPE_PENDING
};

class CoordinatorEvent : public Event {
public:
	CoordinatorEventType type;
	CoordinatorSocket *socket;

	struct {
		struct {
			LOCK_T *lock;
			std::map<struct sockaddr_in, uint32_t> *counter;
			bool *done;
		} remap;
	} message;

	void pending( CoordinatorSocket *socket );
	void syncRemappingRecords( LOCK_T *lock, std::map<struct sockaddr_in, uint32_t> *counter, bool *done );
};

#endif

#ifndef __COORDINATOR_EVENT_COORDINATOR_EVENT_HH__
#define __COORDINATOR_EVENT_COORDINATOR_EVENT_HH__

#include <map>
#include <pthread.h>
#include "../socket/coordinator_socket.hh"
#include "../../common/ds/sockaddr_in.hh"
#include "../../common/event/event.hh"

enum CoordinatorEventType {
	COORDINATOR_EVENT_TYPE_UNDEFINED,
	COORDINATOR_EVENT_TYPE_SYNC_REMAPPED_PARITY,
	COORDINATOR_EVENT_TYPE_PENDING
};

class CoordinatorEvent : public Event {
public:
	CoordinatorEventType type;
	CoordinatorSocket *socket;

	struct {
		struct {
			struct sockaddr_in target;
			pthread_mutex_t *lock;
			pthread_cond_t *cond;
			bool *done;
		} parity;
	} message;

	void pending( CoordinatorSocket *socket );
	void syncRemappedData( struct sockaddr_in target, pthread_mutex_t *lock, pthread_cond_t *cond, bool *done );
};

#endif

#ifndef __COORDINATOR_EVENT_COORDINATOR_EVENT_HH__
#define __COORDINATOR_EVENT_COORDINATOR_EVENT_HH__

#include <map>
#include <pthread.h>
#include "../socket/coordinator_socket.hh"
#include "../../common/ds/sockaddr_in.hh"
#include "../../common/event/event.hh"

enum CoordinatorEventType {
	COORDINATOR_EVENT_TYPE_UNDEFINED,
	COORDINATOR_EVENT_TYPE_SYNC_REMAPPED_PARITY
};

class CoordinatorEvent : public Event<void> {
public:
	CoordinatorEventType type;

	struct {
		struct {
			struct sockaddr_in target;
			pthread_mutex_t *lock;
			pthread_cond_t *cond;
			bool *done;
		} parity;
	} message;

	void syncRemappedData( struct sockaddr_in target, pthread_mutex_t *lock, pthread_cond_t *cond, bool *done );
};

#endif

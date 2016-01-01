#include "coordinator_event.hh"

void CoordinatorEvent::pending( CoordinatorSocket *socket ) {
	this->type = COORDINATOR_EVENT_TYPE_PENDING;
	this->socket = socket;
}

void CoordinatorEvent::syncRemappedData( struct sockaddr_in target, pthread_mutex_t *lock, pthread_cond_t *cond, bool *done ) {
	this->type = COORDINATOR_EVENT_TYPE_SYNC_REMAPPED_PARITY;
	this->message.parity.target = target;
	this->message.parity.lock = lock;
	this->message.parity.cond = cond;
	this->message.parity.done = done;
}

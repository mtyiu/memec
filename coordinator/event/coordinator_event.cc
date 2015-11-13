#include "coordinator_event.hh"

void CoordinatorEvent::pending( CoordinatorSocket *socket ) {
	this->type = COORDINATOR_EVENT_TYPE_PENDING;
	this->socket = socket;
}

void CoordinatorEvent::syncRemappingRecords( LOCK_T *lock, std::map<struct sockaddr_in, uint32_t> *counter, bool *done ) {
	this->type = COORDINATOR_EVENT_TYPE_SYNC_REMAPPING_RECORDS;
	this->message.remap.lock = lock;
	this->message.remap.counter = counter;
	this->message.remap.done = done;
}

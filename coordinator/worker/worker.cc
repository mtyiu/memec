#include "worker.hh"
#include "../../common/util/debug.hh"

#define WORKER_COLOR	YELLOW

void CoordinatorWorker::dispatch( MixedEvent event ) {

}

void CoordinatorWorker::dispatch( ApplicationEvent event ) {

}

void CoordinatorWorker::dispatch( CoordinatorEvent event ) {

}

void CoordinatorWorker::dispatch( MasterEvent event ) {

}

void CoordinatorWorker::dispatch( SlaveEvent event ) {

}


void CoordinatorWorker::free() {

}

void *CoordinatorWorker::run( void *argv ) {
	CoordinatorWorker *worker = ( CoordinatorWorker * ) argv;
	WorkerRole role = worker->getRole();
	CoordinatorEventQueue *eventQueue = worker->getEventQueue();

#define COORDINATOR_WORKER_EVENT_LOOP(_EVENT_TYPE_, _EVENT_QUEUE_) \
	do { \
		_EVENT_TYPE_ event; \
		while( worker->getIsRunning() || _EVENT_QUEUE_->extract( event ) ) { \
			worker->dispatch( event ); \
		} \
	} while( 0 )

	switch ( role ) {
		case WORKER_ROLE_MIXED:
			COORDINATOR_WORKER_EVENT_LOOP(
				MixedEvent,
				eventQueue->mixed
			);
			break;
		case WORKER_ROLE_APPLICATION:
			COORDINATOR_WORKER_EVENT_LOOP(
				ApplicationEvent,
				eventQueue->separated.application
			);
			break;
		case WORKER_ROLE_COORDINATOR:
			COORDINATOR_WORKER_EVENT_LOOP(
				CoordinatorEvent,
				eventQueue->separated.coordinator
			);
			break;
		case WORKER_ROLE_MASTER:
			COORDINATOR_WORKER_EVENT_LOOP(
				MasterEvent,
				eventQueue->separated.master
			);
			break;
		case WORKER_ROLE_SLAVE:
			COORDINATOR_WORKER_EVENT_LOOP(
				SlaveEvent,
				eventQueue->separated.slave
			);
			break;
		default:
			break;
	}

	worker->free();
	pthread_exit( 0 );
	return 0;
}

bool CoordinatorWorker::init( WorkerRole role, CoordinatorEventQueue *eventQueue ) {
	this->role = role;
	this->eventQueue = eventQueue;
	return role != WORKER_ROLE_UNDEFINED;
}

bool CoordinatorWorker::start() {
	this->isRunning = true;
	if ( pthread_create( &this->tid, NULL, CoordinatorWorker::run, ( void * ) this ) != 0 ) {
		__ERROR__( "CoordinatorWorker", "start", "Cannot start worker thread." );
		return false;
	}
	return true;
}

void CoordinatorWorker::stop() {
	this->isRunning = false;
}

void CoordinatorWorker::debug() {
	char role[ 16 ];
	switch( this->role ) {
		case WORKER_ROLE_MIXED:
			strcpy( role, "Mixed" );
			break;
		case WORKER_ROLE_APPLICATION:
			strcpy( role, "Application" );
			break;
		case WORKER_ROLE_COORDINATOR:
			strcpy( role, "Coordinator" );
			break;
		case WORKER_ROLE_MASTER:
			strcpy( role, "Master" );
			break;
		case WORKER_ROLE_SLAVE:
			strcpy( role, "Slave" );
			break;
		default:
			return;
	}
	__DEBUG__( WORKER_COLOR, "CoordinatorWorker", "debug", "%s worker thread #%lu is %srunning.", role, this->tid, this->isRunning ? "" : "not " );
}

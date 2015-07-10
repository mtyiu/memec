#include "worker.hh"
#include "../../common/util/debug.hh"

#define WORKER_COLOR	YELLOW

void MasterWorker::dispatch( MixedEvent event ) {

}

void MasterWorker::dispatch( ApplicationEvent event ) {

}

void MasterWorker::dispatch( CoordinatorEvent event ) {

}

void MasterWorker::dispatch( MasterEvent event ) {

}

void MasterWorker::dispatch( SlaveEvent event ) {

}


void MasterWorker::free() {

}

void *MasterWorker::run( void *argv ) {
	MasterWorker *worker = ( MasterWorker * ) argv;
	WorkerRole role = worker->getRole();
	MasterEventQueue *eventQueue = worker->getEventQueue();

#define MASTER_WORKER_EVENT_LOOP(_EVENT_TYPE_, _EVENT_QUEUE_) \
	do { \
		_EVENT_TYPE_ event; \
		bool ret; \
		while( worker->getIsRunning() | ( ret = _EVENT_QUEUE_->extract( event ) ) ) { \
			if ( ret ) \
				worker->dispatch( event ); \
		} \
	} while( 0 )

	switch ( role ) {
		case WORKER_ROLE_MIXED:
			MASTER_WORKER_EVENT_LOOP(
				MixedEvent,
				eventQueue->mixed
			);
			break;
		case WORKER_ROLE_APPLICATION:
			MASTER_WORKER_EVENT_LOOP(
				ApplicationEvent,
				eventQueue->separated.application
			);
			break;
		case WORKER_ROLE_COORDINATOR:
			MASTER_WORKER_EVENT_LOOP(
				CoordinatorEvent,
				eventQueue->separated.coordinator
			);
			break;
		case WORKER_ROLE_MASTER:
			MASTER_WORKER_EVENT_LOOP(
				MasterEvent,
				eventQueue->separated.master
			);
			break;
		case WORKER_ROLE_SLAVE:
			MASTER_WORKER_EVENT_LOOP(
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

bool MasterWorker::init( GlobalConfig &config, WorkerRole role, MasterEventQueue *eventQueue ) {
	this->protocol.init(
		Protocol::getSuggestedBufferSize(
			config.size.key,
			config.size.chunk
		)
	);
	this->role = role;
	this->eventQueue = eventQueue;
	return role != WORKER_ROLE_UNDEFINED;
}

bool MasterWorker::start() {
	this->isRunning = true;
	if ( pthread_create( &this->tid, NULL, MasterWorker::run, ( void * ) this ) != 0 ) {
		__ERROR__( "MasterWorker", "start", "Cannot start worker thread." );
		return false;
	}
	return true;
}

void MasterWorker::stop() {
	this->isRunning = false;
}

void MasterWorker::debug() {
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
	__DEBUG__( WORKER_COLOR, "MasterWorker", "debug", "%s worker thread #%lu is %srunning.", role, this->tid, this->isRunning ? "" : "not " );
}

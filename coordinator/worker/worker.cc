#include "worker.hh"
#include "../../common/util/debug.hh"

#define WORKER_COLOR	YELLOW

void CoordinatorWorker::free() {

}

void *CoordinatorWorker::run( void *argv ) {
	CoordinatorWorker *worker = ( CoordinatorWorker * ) argv;

	worker->free();
	pthread_exit( 0 );
	return 0;
}

bool CoordinatorWorker::init() {
	return true;
}

bool CoordinatorWorker::start() {
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
	__DEBUG__( WORKER_COLOR, "CoordinatorWorker", "debug", "%s worker thread #%lu is %srunning.\n", role, this->tid, this->isRunning ? "" : "not " );
}

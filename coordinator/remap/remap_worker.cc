#include "remap_msg_handler.hh"
#include "remap_worker.hh"
#include "../../common/util/debug.hh"

#define POLL_ACK_TIME_INTVL	 1   // in seconds

CoordinatorRemapWorker::CoordinatorRemapWorker() {
}

CoordinatorRemapWorker::~CoordinatorRemapWorker() {
}

bool CoordinatorRemapWorker::startRemap( RemapStatusEvent event ) {
	CoordinatorRemapMsgHandler *crmh = CoordinatorRemapMsgHandler::getInstance();

	// wait for ack (busy waiting)
	while( crmh->isAllMasterAcked( event.slave ) == false )
		sleep( POLL_ACK_TIME_INTVL );

	LOCK( &crmh->slavesStatusLock[ event.slave ] );
	// for multi-threaded environment, it is possible that other workers change 
	// the status while this worker is waiting
	// just abort and late comers to take over
	if ( crmh->slaveStatus[ event.slave ] != REMAP_PREPARE_START ) {
		UNLOCK( &crmh->slavesStatusLock[ event.slave ] );
		return false;
	}
	// any cleanup to be done
	crmh->startRemapEnd( event.slave );

	// ask master to start remap
	if ( crmh->sendStatusToMasters( event.slave ) == false ) {
		UNLOCK( &crmh->slavesStatusLock[ event.slave ] );
		return false;
	}

	crmh->slaveStatus[ event.slave ] = REMAP_START;
	UNLOCK( &crmh->slavesStatusLock[ event.slave ] );
	return true;
}

bool CoordinatorRemapWorker::stopRemap( RemapStatusEvent event ) {
	CoordinatorRemapMsgHandler *crmh = CoordinatorRemapMsgHandler::getInstance();

	// wait for ack.
	while( crmh->isAllMasterAcked( event.slave ) == false )
		sleep( POLL_ACK_TIME_INTVL );

	LOCK( &crmh->slavesStatusLock[ event.slave ] );
	// for multi-threaded environment, it is possible that other workers change 
	// the tatus while this worker is waiting. 
	// just abort and late comers to take over
	if ( crmh->slaveStatus[ event.slave ] != REMAP_PREPARE_END ) {
		UNLOCK( &crmh->slavesStatusLock[ event.slave ] );
		return false;
	}
	// any cleanup to be done
	crmh->stopRemapEnd( event.slave );

	// ask master to use normal SET workflow
	if ( crmh->sendStatusToMasters( event.slave ) == false ) {
		UNLOCK( &crmh->slavesStatusLock[ event.slave ] );
		return false;
	}
	crmh->slaveStatus[ event.slave ] = REMAP_NONE ;
	UNLOCK( &crmh->slavesStatusLock[ event.slave ] );
	return false;
}

void *CoordinatorRemapWorker::run( void* argv ) {
	CoordinatorRemapMsgHandler *crmh = CoordinatorRemapMsgHandler::getInstance();
	CoordinatorRemapWorker *worker = ( CoordinatorRemapWorker *) argv;
	RemapStatusEvent event;
	
	while ( worker->getIsRunning()) {
		if ( ! crmh->eventQueue->extract( event ) )
			break;
		if ( event.start )
			worker->startRemap( event );
		else
			worker->stopRemap( event );
	}

	worker->free();
	pthread_exit( 0 );
	return 0;
}

bool CoordinatorRemapWorker::start() {
	if ( ! this->isRunning ) {
		this->isRunning = true;
		if ( pthread_create( &this->tid, NULL, CoordinatorRemapWorker::run, ( void * ) this ) != 0 ) {
			__ERROR__( "CoordinatorRemapWorker", "start", "Cannot start worker thread." );
			return false;
		}
	}
	return true;
}

void CoordinatorRemapWorker::stop() {
	this->isRunning = false;
}

void CoordinatorRemapWorker::free() {
}

void CoordinatorRemapWorker::print( FILE *f ) {
	// TODO
}

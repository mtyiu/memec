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
	// check if there is a collision in status
	LOCK( &crmh->slavesStatusLock[ event.slave ] );
	if ( crmh->slaveStatus[ event.slave ] != REMAP_NONE &&
			crmh->slaveStatus[ event.slave ] != REMAP_PREPARE_START ) {
		UNLOCK( &crmh->slavesStatusLock[ event.slave ] );
		return false;
	}

	crmh->resetMasterAck( event.slave );

	// ask master to prepare for start
	if ( crmh->sendMessageToMasters( REMAP_PREPARE_START ) == false ) {
		UNLOCK( &crmh->slavesStatusLock[ event.slave ] );
		return false;
	}
	crmh->slaveStatus[ event.slave ] = REMAP_PREPARE_START;
	UNLOCK( &crmh->slavesStatusLock[ event.slave ] );

	// wait for ack (busy waiting)
	while( crmh->isAllMasterAcked( event.slave ) == false )
		sleep( POLL_ACK_TIME_INTVL );

	// ask master to start remap
	LOCK( &crmh->slavesStatusLock[ event.slave ] );
	if ( crmh->sendMessageToMasters( REMAP_START ) == false ) {
		UNLOCK( &crmh->slavesStatusLock[ event.slave ] );
		return false;
	}
	crmh->slaveStatus[ event.slave ] = REMAP_START;
	UNLOCK( &crmh->slavesStatusLock[ event.slave ] );
	return true;
}

bool CoordinatorRemapWorker::stopRemap( RemapStatusEvent event ) {
	CoordinatorRemapMsgHandler *crmh = CoordinatorRemapMsgHandler::getInstance();

	LOCK( &crmh->slavesStatusLock[ event.slave ] );
	if ( crmh->slaveStatus[ event.slave ] != REMAP_START &&
			crmh->slaveStatus[ event.slave ] != REMAP_PREPARE_END ) {
		UNLOCK( &crmh->slavesStatusLock[ event.slave ] );
		return false;
	}

	crmh->resetMasterAck( event.slave );

	// ask master to stop remapping
	if ( crmh->sendMessageToMasters( REMAP_PREPARE_END ) == false ) {
		UNLOCK( &crmh->slavesStatusLock[ event.slave ] );
		return false;
	}
	crmh->slaveStatus[ event.slave ] = REMAP_PREPARE_END ;
	UNLOCK( &crmh->slavesStatusLock[ event.slave ] );

	// TODO wait for ack.
	while( crmh->isAllMasterAcked( event.slave ) == false )
		sleep(1);

	// ask master to use normal SET workflow
	LOCK( &crmh->slavesStatusLock[ event.slave ] );
	if ( crmh->sendMessageToMasters( REMAP_END ) == false ) {
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

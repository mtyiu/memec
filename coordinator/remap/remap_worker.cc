#include "remap_msg_handler.hh"
#include "remap_worker.hh"
#include "../../common/util/debug.hh"
#include "arpa/inet.h"

#define POLL_ACK_TIME_INTVL	 1   // in seconds

CoordinatorRemapWorker::CoordinatorRemapWorker() {
}

CoordinatorRemapWorker::~CoordinatorRemapWorker() {
}

bool CoordinatorRemapWorker::startRemap( RemapStatusEvent event ) {
	CoordinatorRemapMsgHandler *crmh = CoordinatorRemapMsgHandler::getInstance();

	pthread_mutex_t lock;
	pthread_mutex_init( &lock, NULL );
	char buf[ INET_ADDRSTRLEN ];
	inet_ntop( AF_INET, &event.slave.sin_addr.s_addr, buf, INET_ADDRSTRLEN );
	// wait for ack, release lock once acquired
	if ( crmh->isAllMasterAcked( event.slave ) == false )
		pthread_cond_wait( &crmh->ackSignal[ event.slave ], &crmh->ackSignalLock );
	UNLOCK( &crmh->ackSignalLock );

	LOCK( &crmh->slavesStatusLock[ event.slave ] );
	// for multi-threaded environment, it is possible that other workers change 
	// the status while this worker is waiting
	// just abort and late comers to take over
	if ( crmh->slavesStatus[ event.slave ] != REMAP_PREPARE_START ) {
		UNLOCK( &crmh->slavesStatusLock[ event.slave ] );
		return false;
	}
	// any cleanup to be done
	crmh->startRemapEnd( event.slave );

	// ask master to start remap
	crmh->slavesStatus[ event.slave ] = REMAP_START;
	if ( crmh->sendStatusToMasters( event.slave ) == false ) {
		__ERROR__( "CoordinatorRemapWorker", "startRemap", "Failed to broadcast status changes!" );
		UNLOCK( &crmh->slavesStatusLock[ event.slave ] );
		return false;
	}

	UNLOCK( &crmh->slavesStatusLock[ event.slave ] );
	return true;
}

bool CoordinatorRemapWorker::stopRemap( RemapStatusEvent event ) {
	CoordinatorRemapMsgHandler *crmh = CoordinatorRemapMsgHandler::getInstance();

	pthread_mutex_t lock;
	pthread_mutex_init( &lock, NULL );
	char buf[ INET_ADDRSTRLEN ];
	inet_ntop( AF_INET, &event.slave.sin_addr.s_addr, buf, INET_ADDRSTRLEN );
	// wait for ack, release lock once acquired
	if ( crmh->isAllMasterAcked( event.slave ) == false )
		pthread_cond_wait( &crmh->ackSignal[ event.slave ], &crmh->ackSignalLock );
	UNLOCK( &crmh->ackSignalLock );

	LOCK( &crmh->slavesStatusLock[ event.slave ] );
	// for multi-threaded environment, it is possible that other workers change 
	// the tatus while this worker is waiting. 
	// just abort and late comers to take over
	if ( crmh->slavesStatus[ event.slave ] != REMAP_PREPARE_END ) {
		UNLOCK( &crmh->slavesStatusLock[ event.slave ] );
		return false;
	}
	// any cleanup to be done
	crmh->stopRemapEnd( event.slave );

	// ask master to use normal SET workflow
	crmh->slavesStatus[ event.slave ] = REMAP_NONE ;
	if ( crmh->sendStatusToMasters( event.slave ) == false ) {
		__ERROR__( "CoordinatorRemapWorker", "stopRemap", "Failed to broadcast status changes!" );
		UNLOCK( &crmh->slavesStatusLock[ event.slave ] );
		return false;
	}
	UNLOCK( &crmh->slavesStatusLock[ event.slave ] );
	return true;
}

void *CoordinatorRemapWorker::run( void* argv ) {
	CoordinatorRemapMsgHandler *crmh = CoordinatorRemapMsgHandler::getInstance();
	CoordinatorRemapWorker *worker = ( CoordinatorRemapWorker *) argv;
	RemapStatusEvent event;
	bool ret;
	
	while ( true ) {
		ret = crmh->eventQueue->extract( event );
		if ( ! ret ) {
			if ( ! worker->getIsRunning() ) {
				break;
			}
			continue;
		}
		if ( event.start )
			worker->startRemap( event );
		else
			worker->stopRemap( event );
	}

	__DEBUG__( BLUE, "CoordinatorRemapWorker", "run" "Worker thread stop running." );
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
